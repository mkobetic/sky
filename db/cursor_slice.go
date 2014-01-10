package db

import "github.com/szferi/gomdb"

// Cursors is a list of LMDB cursor objects.
type Cursors []*mdb.Cursor

// Close deallocates all cursor resources.
func (s Cursors) Close() {
	for _, c := range s {
		txn := c.Txn()
		c.Close()
		txn.Commit()
	}
}
