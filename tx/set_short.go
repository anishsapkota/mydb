package tx

import (
	"fmt"
	"mydb/file"
	"mydb/log"
	"mydb/utils"
)

type SetShortRecord struct {
	LogRecord
	txNum  int
	offset int
	value  int16
	block  *file.BlockId
}

func NewSetShortRecord(page *file.Page) (*SetShortRecord, error) {
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
	val := page.GetShort(valuePos)

	return &SetShortRecord{txNum: txNum, offset: offset, value: val, block: block}, nil
}

func (r *SetShortRecord) Op() LogRecordType {
	return SetShort
}

func (r *SetShortRecord) TxNumber() int {
	return r.txNum
}

func (r *SetShortRecord) String() string {
	return fmt.Sprintf("<SETSHORT %d %s %d %d>", r.txNum, r.block, r.offset, r.value)
}

func (r *SetShortRecord) Undo(tx *Transaction) error {
	if err := tx.Pin(r.block); err != nil {
		return err
	}
	defer tx.Unpin(r.block)
	return tx.SetShort(r.block, r.offset, r.value, false)
}

func WriteSetShortToLog(logManager *log.Manager, txNum int, block *file.BlockId, offset int, val int16) (int, error) {
	operationPos := 0
	txNumPos := operationPos + utils.IntSize
	fileNamePos := txNumPos + utils.IntSize
	fileName := block.Filename()

	blockNumPos := fileNamePos + file.MaxLength(len(fileName))
	blockNum := block.Number()

	offsetPos := blockNumPos + utils.IntSize
	valuePos := offsetPos + utils.IntSize
	// int16 is 2 bytes
	recordLen := valuePos + 2

	recordBytes := make([]byte, recordLen)
	page := file.NewPageFromBytes(recordBytes)

	page.SetInt(operationPos, int(SetShort))
	page.SetInt(txNumPos, txNum)
	if err := page.SetString(fileNamePos, fileName); err != nil {
		return -1, err
	}
	page.SetInt(blockNumPos, blockNum)
	page.SetInt(offsetPos, offset)
	page.SetShort(valuePos, val)

	return logManager.Append(recordBytes)
}
