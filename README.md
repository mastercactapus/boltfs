# boltfs

BoltFS allows storing and streaming files in Bolt. The goal of the project is to provide a simple way to store larger payloads in chunks, and expose an API compatible with `http.FileServer`.

When reading or writing files care is taken to ensure consistancy. Similarly to Bolt, a reader will always see the same view of a file, and created files will update atomically on `Close`. The rules are meant to feel the same or similar as bolt itself.

## Experimental Code

This project is still in its experimental phase, tests are still being written, and things are still changing. This warning will be lifted once things have been ironed out and it is being used in production.

## Installing

```bash

go get github.com/mastercactapus/boltfs
```

## File Server Example

```go
package main

import (
	"github.com/boltdb/bolt"
	"github.com/mastercactapus/boltfs"
	"log"
	"net/http"
)

func main() {
	db, err := bolt.Open("foo.db", 0644, nil)
	if err != nil {
		log.Fatalln(err)
	}
	fs, err := boltfs.NewFileSystem(boltfs.NewBoltDB(db), boltfs.NewBucketPath([]byte("data")))
	if err != nil {
		log.Fatalln(err)
	}

	log.Fatalln(http.ListenAndServe(":8000", http.FileServer(fs)))
}

```
