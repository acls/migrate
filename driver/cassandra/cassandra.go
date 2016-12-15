// Package cassandra implements the Driver interface.
package cassandra

import (
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/acls/migrate/driver"
	"github.com/acls/migrate/file"
	"github.com/acls/migrate/migrate/direction"
	"github.com/gocql/gocql"
)

type Driver struct {
	session *gocql.Session
}

const (
	tableName  = "schema_migrations"
	versionRow = 1
)

type counterStmt bool

func (c counterStmt) String() string {
	sign := ""
	if bool(c) {
		sign = "+"
	} else {
		sign = "-"
	}
	return "UPDATE " + tableName + " SET version = version " + sign + " 1 where versionRow = ?"
}

const (
	up   counterStmt = true
	down counterStmt = false
)

// Cassandra Driver URL format:
// cassandra://host:port/keyspace?protocol=version
//
// Example:
// cassandra://localhost/SpaceOfKeys?protocol=4
func (driver *Driver) Initialize(rawurl string) error {
	u, err := url.Parse(rawurl)

	cluster := gocql.NewCluster(u.Host)
	cluster.Keyspace = u.Path[1:len(u.Path)]
	cluster.Consistency = gocql.All
	cluster.Timeout = 1 * time.Minute

	if len(u.Query().Get("protocol")) > 0 {
		protoversion, err := strconv.Atoi(u.Query().Get("protocol"))
		if err != nil {
			return err
		}

		cluster.ProtoVersion = protoversion
	}

	// Check if url user struct is null
	if u.User != nil {
		password, passwordSet := u.User.Password()

		if passwordSet == false {
			return fmt.Errorf("Missing password. Please provide password.")
		}

		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: u.User.Username(),
			Password: password,
		}

	}

	driver.session, err = cluster.CreateSession()

	if err != nil {
		return err
	}

	if err := driver.ensureVersionTableExists(); err != nil {
		return err
	}
	return nil
}

func (driver *Driver) Close() error {
	driver.session.Close()
	return nil
}

func (driver *Driver) ensureVersionTableExists() error {
	err := driver.session.Query("CREATE TABLE IF NOT EXISTS " + tableName + " (version counter, versionRow bigint primary key);").Exec()
	if err != nil {
		return err
	}

	_, err = driver.Version()
	if err != nil {
		driver.session.Query(up.String(), versionRow).Exec()
	}

	return nil
}

func (driver *Driver) FilenameExtension() string {
	return "cql"
}

func (driver *Driver) version(d direction.Direction, invert bool) error {
	var stmt counterStmt
	switch d {
	case direction.Up:
		stmt = up
	case direction.Down:
		stmt = down
	}
	if invert {
		stmt = !stmt
	}

	return driver.session.Query(stmt.String(), versionRow).Exec()
}

func (driver *Driver) Begin() (driver.Tx, error) {
	startVersion, err := driver.Version()
	if err != nil {
		return nil, err
	}
	return &cassandraTx{driver.session, startVersion}, nil
}

func (driver *Driver) Migrate(tx driver.Tx, f file.File, pipe chan interface{}) {
	var err error
	defer func() {
		if err != nil {
			pipe <- err
			if err := tx.Rollback(); err != nil {
				pipe <- err
			}
		}
		close(pipe)
	}()
	pipe <- f

	if err = driver.version(f.Direction, false); err != nil {
		return
	}

	if err = f.ReadContent(); err != nil {
		return
	}

	if _, err = tx.Exec(string(f.Content)); err != nil {
		return
	}

	if err = tx.Commit(); err != nil {
		return
	}
}

func (driver *Driver) Version() (uint64, error) {
	return getVersion(driver.session)
}

func init() {
	driver.RegisterDriver("cassandra", &Driver{})
}

type cassandraTx struct {
	session      *gocql.Session
	startVersion uint64
}

func (tx *cassandraTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	for _, query := range strings.Split(query, ";") {
		query = strings.TrimSpace(query)
		if len(query) == 0 {
			continue
		}

		if err := tx.session.Query(query, args...).Exec(); err != nil {
			return nil, err
		}
	}
	return cassandraResult{}, nil
}
func (tx *cassandraTx) Rollback() error {
	return setVersion(tx.session, tx.startVersion)
}
func (tx *cassandraTx) Commit() error {
	return nil
}

func getVersion(session *gocql.Session) (uint64, error) {
	var version int64
	err := session.Query("SELECT version FROM "+tableName+" WHERE versionRow = ?", versionRow).Scan(&version)
	return uint64(version) - 1, err
}
func setVersion(session *gocql.Session, version uint64) error {
	currVersion, err := getVersion(session)
	if err != nil {
		return err
	}

	var (
		sign  string
		delta uint64
	)
	if currVersion < version {
		sign = "+"
		delta = version - currVersion
	} else {
		sign = "-"
		delta = currVersion - version
	}
	return session.Query("UPDATE "+tableName+" SET version = version "+sign+" ? where versionRow = ?", delta, versionRow).Exec()
}

type cassandraResult struct{}

func (r cassandraResult) LastInsertId() (int64, error) {
	return 0, nil
}
func (r cassandraResult) RowsAffected() (int64, error) {
	return 0, nil
}
