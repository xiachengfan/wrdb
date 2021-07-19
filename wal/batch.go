package wal

type Batch struct {
	entries []batchEntry
	datas   []byte
}

type batchEntry struct {
	index uint64
	size  int
}

// Write an entry to the batch
func (b *Batch) Write(index uint64, data []byte) {
	b.entries = append(b.entries, batchEntry{index, len(data)})
	b.datas = append(b.datas, data...)

}

// Clear the batch for reuse.
func (b *Batch) Clear() {
	b.entries = b.entries[:0]
	b.datas = b.datas[:0]
}
