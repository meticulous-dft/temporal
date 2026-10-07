package main

import (
	"fmt"
	"os"

	"go.temporal.io/server/tools/mongodb"
)

func main() {
	if err := mongodb.RunTool(os.Args); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
