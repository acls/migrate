// Package txtype just holds constants for per file and single transaction types.
package txtype

type TxType int

const (
	TxNone TxType = iota
	TxPerFile
	TxSingle
)
