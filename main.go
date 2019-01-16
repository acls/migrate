// Package main is the CLI.
// You can use the CLI via Terminal.
// import "github.com/acls/migrate/migrate" for usage within Go.
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	mpgx "github.com/acls/migrate/driver/pgx"

	"github.com/acls/migrate/driver"
	"github.com/acls/migrate/file"
	"github.com/acls/migrate/migrate"
	"github.com/acls/migrate/migrate/direction"
	pipep "github.com/acls/migrate/pipe"
	"github.com/fatih/color"
)

const Version string = "2.0.0"

func main() {
	m := &migrate.Migrator{
		Driver:     mpgx.New(""),
		Interrupts: true,
	}

	var url string
	flag.StringVar(&url, "url", os.Getenv("MIGRATE_URL"), "")
	flag.StringVar(&m.Path, "path", os.Getenv("SCHEMA_DIR"), "")
	flag.StringVar(&m.PrevPath, "prev", os.Getenv("PREV_SCHEMA_DIR"), "")
	flag.BoolVar(&m.TxPerFile, "perfile", false, "")
	flag.BoolVar(&m.Force, "force", false, "")
	var incMajor bool
	flag.BoolVar(&incMajor, "major", false, "")
	var version bool
	flag.BoolVar(&version, "version", false, "")

	flag.Usage = func() {
		printHelp()
	}

	flag.Parse()
	command := flag.Arg(0)
	if version {
		fmt.Println(Version)
		os.Exit(0)
	}

	if url == "" {
		fmt.Println("No url")
		os.Exit(0)
	}

	conn, err := m.Driver.NewConn(url)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if m.Path == "" {
		m.Path, _ = os.Getwd()
	}
	if m.PrevPath == "" {
		m.PrevPath = m.Path + "-prev"
	}

	switch command {
	default:
		runMigration(m, conn, command)
	case "create":
		name := flag.Arg(1)
		if name == "" {
			fmt.Println("Please specify name.")
			os.Exit(1)
		}
		migrationFile, err := m.Create(incMajor, name)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Printf("Create version %s/%v migration files\n", m.Path, migrationFile.Version)
		fmt.Println(migrationFile.UpFile.FileName)
		fmt.Println(migrationFile.DownFile.FileName)
		os.Exit(0)
	case "version":
		printComplete(m, conn, time.Now())
		os.Exit(0)
	case "help":
		printHelp()
		os.Exit(0)
	}
}
func runMigration(m *migrate.Migrator, conn driver.Conn, command string) {
	timerStart := time.Now()
	pipe := pipep.New()

	switch command {
	default:
		printHelp()
		os.Exit(0)

	case "migrate":
		relativeN := flag.Arg(1)
		relativeNInt, err := strconv.Atoi(relativeN)
		if err != nil {
			fmt.Println("Unable to parse param <n>.")
			os.Exit(1)
		}
		go m.Migrate(pipe, conn, relativeNInt)
	case "between":
		if m.PrevPath == m.Path {
			fmt.Println("'-prev' must not be the same as '-path'")
			os.Exit(1)
		}
		go m.MigrateBetween(pipe, conn)
	case "goto":
		var toVersion file.Version
		if err := toVersion.Parse(flag.Arg(1)); err != nil {
			fmt.Println("Unable to parse param <v>.", err)
			os.Exit(1)
		}
		go m.MigrateTo(pipe, conn, toVersion)
	case "up":
		go m.Up(pipe, conn)
	case "down":
		go m.Down(pipe, conn)
	case "redo":
		go m.Redo(pipe, conn)
	case "reset":
		go m.Reset(pipe, conn)
	}

	ok := writePipe(pipe)
	printComplete(m, conn, timerStart)
	if !ok {
		os.Exit(1)
	}
}

func writePipe(pipe chan interface{}) (ok bool) {
	okFlag := true
	if pipe != nil {
		for {
			select {
			case item, more := <-pipe:
				if !more {
					return okFlag
				} else {
					switch item.(type) {

					case string:
						fmt.Println(item.(string))

					case error:
						c := color.New(color.FgRed)
						c.Println(item.(error).Error())
						okFlag = false

					case *file.File:
						f := item.(*file.File)
						var c *color.Color
						var d string
						switch f.Direction {
						case direction.Up:
							c = color.New(color.FgGreen)
							d = ">"
						case direction.Down:
							c = color.New(color.FgBlue)
							d = "<"
						default:
							c = color.New(color.FgBlack)
							d = "-"
						}
						c.Printf("%s %v/%s\n", d, f.MajorString(), f.FileName)

					default:
						text := fmt.Sprint(item)
						fmt.Println(text)
					}
				}
			}
		}
	}
	return okFlag
}

func printComplete(m *migrate.Migrator, conn driver.Conn, timerStart time.Time) {
	var version string
	v, err := m.Driver.Version(conn)
	if err != nil {
		version = err.Error()
	} else {
		version = v.String()
	}

	var duration string
	diff := time.Now().Sub(timerStart).Seconds()
	if diff > 60 {
		duration = fmt.Sprintf("%.4f minutes", diff/60)
	} else {
		duration = fmt.Sprintf("%.4f seconds", diff)
	}

	fmt.Printf(`
Schema Version: %s
      Duration: %s
`,
		version,
		duration,
	)
}

func printHelp() {
	os.Stderr.WriteString(
		`usage: migrate [-prev=<prev>] [-path=<path>] -url=<url> <command> [<args>]

Commands:
   create <name>  Create a new migration
   up             Apply all -up- migrations
   down           Apply all -down- migrations
   reset          Down followed by Up
   redo           Roll back most recent migration, then apply it again
   version        Show current migration version
   migrate <n>    Apply migrations -n|+n
   goto <v>       Migrate to version v
   between        Migrates between '-prev' and '-path'
   help           Show this help

'-version'  Print version then exit.
'-path'     Defaults to current working directory.
'-prev'     Directory to store migrated schemas. Defaults to <path>-prev.
'-perfile'  Per file transaction. Defaults to one transaction per major version.
'-major'    Increment major version. Applies to 'create' command.
'-force'    Skips validation. Applies to 'between' command.
`)
}
