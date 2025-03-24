package tx

import (
	"fmt"
	"mydb/file"
	"mydb/log"
	"mydb/utils"
)

type StartRecord struct {
	LogRecord
	txNum int
}

func NewStartRecord(page *file.Page) (*StartRecord, error) {
	operationPos := 0
	txNumPos := operationPos + utils.IntSize
	txNum := page.GetInt(txNumPos)
	return &StartRecord{txNum: txNum}, nil
}

// Op returns the type of the log record.
func (r *StartRecord) Op() LogRecordType {
	return Start
}

// TxNumber returns the transaction number stored in the log record.
func (r *StartRecord) TxNumber() int {
	return r.txNum
}

// Undo does nothing. StartRecord does not change any data.
func (r *StartRecord) Undo(_ *Transaction) error {
	return nil
}

// String returns a string representation of the log record.
func (r *StartRecord) String() string {
	return fmt.Sprintf("<START %d>", r.txNum)
}

func WriteStartToLog(logManager *log.Manager, txNum int) (int, error) {
	record := make([]byte, 2*utils.IntSize)

	page := file.NewPageFromBytes(record)
	page.SetInt(0, int(Start))
	page.SetInt(4, txNum)

	return logManager.Append(record)
}
