package tx

import (
	"fmt"
	"mydb/file"
	"mydb/log"
	"mydb/utils"
)

type RollbackRecord struct {
	LogRecord
	txNum int
}

func NewRollbackRecord(page *file.Page) (*RollbackRecord, error) {
	operationPos := 0
	txNumPos := operationPos * utils.IntSize
	txNum := int(page.GetInt(txNumPos))
	return &RollbackRecord{
		txNum: txNum,
	}, nil
}

// Op returns the type of the log record.
func (r *RollbackRecord) Op() LogRecordType {
	return Rollback
}

// TxNumber returns the transaction number stored in the log record.
func (r *RollbackRecord) TxNumber() int {
	return r.txNum
}

// Undo does nothing. RollbackRecord does not change any data.
func (r *RollbackRecord) Undo(_ *Transaction) error {
	return nil
}

// String returns a string representation of the log record.
func (r *RollbackRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d>", r.txNum)
}

// WriteRollbackToLog writes a rollback record to the log. This log record contains the Rollback operator,
// followed by the transaction id.
// The method returns the LSN of the new log record.
func WriteRollbackToLog(logManager *log.Manager, txNum int) (int, error) {
	record := make([]byte, 2*utils.IntSize)

	page := file.NewPageFromBytes(record)
	page.SetInt(0, int(Rollback))
	page.SetInt(4, txNum)

	return logManager.Append(record)
}
