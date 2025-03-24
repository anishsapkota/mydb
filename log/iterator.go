package log

import (
	"errors"
	"fmt"
	"mydb/file"
)

// Iterator provides the ability to move through the records of the log files in reverse order
type Iterator struct {
	fileManager     *file.Manager
	block           *file.BlockId
	page            *file.Page
	currentPosition int
	boundary        int
}

// NewIterator creates an iterator for the records in the log file, positioned after the last log record.
func NewIterator(fileManager *file.Manager, block *file.BlockId) (*Iterator, error) {
	page := file.NewPage(fileManager.BlockSize())
	iterator := &Iterator{
		fileManager: fileManager,
		block:       block,
		page:        page,
	}
	if err := iterator.moveToBlock(block); err != nil {
		return nil, fmt.Errorf("failed to move to block: %v", err)
	}

	return iterator, nil
}

// HasNext determines if the current log record is the earliest record in the log file. Returns true if there is an earlier record.
func (it *Iterator) HasNext() bool {
	return it.currentPosition < it.fileManager.BlockSize() || it.block.Number() > 0
}

// Next moves to the next log record in the block.
// If there are no more log records in the block, then move to the previous block and return the log record from there.
// Returns the next earliest log record.
func (it *Iterator) Next() ([]byte, error) {
	if it.currentPosition == it.fileManager.BlockSize() {
		if it.block.Number() == 0 {
			return nil, errors.New("no more log records")
		}
		it.block = &file.BlockId{File: it.block.Filename(), BlockNumber: it.block.Number() - 1}
		if err := it.moveToBlock(it.block); err != nil {
			return nil, fmt.Errorf("failed to move to block :%v", err)
		}

	}
	record := it.page.GetBytes(it.currentPosition)
	it.currentPosition += 4 + len(record) // (size if record) + (length of record)
	return record, nil
}

func (it *Iterator) moveToBlock(block *file.BlockId) error {
	if err := it.fileManager.Read(block, it.page); err != nil {
		return fmt.Errorf("failed to read block: %v", err)
	}

	it.boundary = int(it.page.GetInt(0))
	it.currentPosition = it.boundary
	return nil
}
