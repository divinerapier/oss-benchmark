package writer

import (
	"io"
)

type DiscardWriterAt struct {
	io.WriterAt
}

func (w *DiscardWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	return len(p), nil
}
