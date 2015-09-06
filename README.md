# boltfs

BoltFS allows storing and streaming files in Bolt. The goal of the project is to provide a simple way to store larger payloads in chunks, and expose an API compatible with `http.FileServer`.

When reading or writing files care is taken to ensure consistancy. Similarly to Bolt, a reader will always see the same view of a file, and created files will update atomically on `Close`. The rules are meant to feel the same or similar as bolt itself.

## Installing

```bash

go get github.com/mastercactapus/boltfs
```

## File Server Example

```go
package main

import (
	"http"
	"github.com/mastercactapus/boltfs"
	
)