package tx

import (
	"fmt"
	"mydb/file"
	"mydb/log"
	"mydb/utils"
)

type SetBoolRecord struct {
	LogRecord
	txNum  int
	offset int
	value  bool
	block  *file.BlockId
}

func NewSetBoolRecord(page *file.Page) (*SetBoolRecord, error) {
	operationPos := 0
	txNumPos := operationPos + utils.IntSize
	txNum := page.GetInt(txNumPos)

	fileNamePos := txNumPos + utils.IntSize
	fileName, err := page.GetString(fileNamePos)
	if err != nil {
		return nil, err
	}

	blockNumPos := fileNamePos + file.MaxLength(len(fileName))
	blockNum := page.GetInt(blockNumPos)
	block := &file.BlockId{File: fileName, BlockNumber: int(blockNum)}

	offsetPos := blockNumPos + utils.IntSize
	offset := page.GetInt(offsetPos)

	valuePos := offsetPos + utils.IntSize
	val := page.GetBool(valuePos)

	return &SetBoolRecord{txNum: txNum, offset: offset, value: val, block: block}, nil
}

func (r *SetBoolRecord) Op() LogRecordType {
	return SetBool
}

func (r *SetBoolRecord) TxNumber() int {
	return r.txNum
}

func (r *SetBoolRecord) String() string {
	return fmt.Sprintf("<SETBOOL %d %s %d %t>", r.txNum, r.block, r.offset, r.value)
}

func (r *SetBoolRecord) Undo(tx *Transaction) error {
	if err := tx.Pin(r.block); err != nil {
		return err
	}
	defer tx.Unpin(r.block)
	return tx.SetBool(r.block, r.offset, r.value, false)
}

func WriteSetBoolToLog(logManager *log.Manager, txNum int, block *file.BlockId, offset int, val bool) (int, error) {
	operationPos := 0
	txNumPos := operationPos + utils.IntSize
	fileNamePos := txNumPos + utils.IntSize
	fileName := block.Filename()

	blockNumPos := fileNamePos + file.MaxLength(len(fileName))
	blockNum := block.Number()

	offsetPos := blockNumPos + utils.IntSize
	valuePos := offsetPos + utils.IntSize

	// 1 byte for bool
	recordLen := valuePos + 1

	recordBytes := make([]byte, recordLen)
	page := file.NewPageFromBytes(recordBytes)

	page.SetInt(operationPos, int(SetBool))
	page.SetInt(txNumPos, txNum)
	if err := page.SetString(fileNamePos, fileName); err != nil {
		return -1, err
	}
	page.SetInt(blockNumPos, blockNum)
	page.SetInt(offsetPos, offset)
	page.SetBool(valuePos, val)

	return logManager.Append(recordBytes)
}
