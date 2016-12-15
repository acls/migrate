package migrate

import (
	"database/sql"
	"fmt"

	"github.com/acls/migrate/driver"
	"github.com/acls/migrate/migrate/txtype"
)

// transaction, depending on the transaction type,
// creates a new transaction for each migration file
// or a single transaction for all migration files
type transaction struct {
	d   driver.Driver
	typ txtype.TxType
	tx  driver.Tx
}

func (t *transaction) newTx() error {
	if t.tx != nil {
		return fmt.Errorf("transaction already exists")
	}

	tx, err := t.d.Begin()
	if err != nil {
		return err
	}
	t.tx = tx
	return nil
}

func (t *transaction) Begin() (driver.Tx, error) {
	switch t.typ {
	default: // case txtype.TxPerFile:
		return t, t.newTx()

	case txtype.TxSingle:
		// ensure a transaction exists
		if t.tx == nil {
			err := t.newTx()
			if err != nil {
				return nil, err
			}
		}
		//
		return t, nil
	}
}
func (t *transaction) Exec(query string, args ...interface{}) (sql.Result, error) {
	if t.tx == nil {
		return nil, fmt.Errorf("no transaction")
	}
	return t.tx.Exec(query, args...)
}
func (t *transaction) Rollback() error {
	if t.tx == nil {
		return fmt.Errorf("no transaction")
	}
	return t.tx.Rollback()
}
func (t *transaction) Commit() error {
	switch t.typ {
	default: // case txtype.TxPerFile:
		if t.tx == nil {
			return fmt.Errorf("no transaction")
		}
		// commit transaction and remove reference to it so a new one can be started
		tx := t.tx
		t.tx = nil
		return tx.Commit()

	case txtype.TxSingle:
		return nil
	}
}
func (t *transaction) CommitAll() error {
	switch t.typ {
	default: // case txtype.TxPerFile:
		return nil

	case txtype.TxSingle:
		if t.tx == nil {
			return fmt.Errorf("no transaction")
		}
		// commit all migrations
		return t.tx.Commit()
	}
}
