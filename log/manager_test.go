package log

import (
	"fmt"
	"mydb/file"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Helper function to create a new temporary FileMgr
func createTempFileMgr(blocksize int) (*file.Manager, func(), error) {
	tempDir, err := os.MkdirTemp("", "filemgr_test")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create temp directory: %v", err)

	}

	fm, err := file.NewManager(tempDir, blocksize)
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, nil, fmt.Errorf("failed to create FileMgr: %v", err)
	}

	cleanup := func() { os.RemoveAll(tempDir) }

	return fm, cleanup, nil
}

func TestLogMgr_AppendAndIteratorConsistency(t *testing.T) {
	assert := assert.New(t)
	blockSize := 4096
	fm, cleanup, err := createTempFileMgr(blockSize)

	defer cleanup()
	assert.NoError(err)

	logfile := "testlog"

	lm, err := NewManager(fm, logfile)
	assert.NoError(err)

	//Append and flush multiple records, then verify consistency
	recordCount := 100
	records := make([][]byte, recordCount)
	for i := 0; i < recordCount; i++ {
		records[i] = []byte(fmt.Sprintf("Log record %d", i+1))
		_, err := lm.Append(records[i])
		assert.NoErrorf(err, "Error appending record %d: %v", i+1, err)
	}

	// Verify with iterator in reverse order

	iterator, err := lm.Iterator()
	assert.NoError(err)

	for i := recordCount - 1; i >= 0; i-- {
		assert.Truef(iterator.HasNext(), "Expected more records, but iterator has none")
		rec, err := iterator.Next()
		assert.NoError(err)

		assert.Equal(rec, records[i])
	}

	assert.Falsef(iterator.HasNext(), "Expected no more records, but iterator has more")
}
