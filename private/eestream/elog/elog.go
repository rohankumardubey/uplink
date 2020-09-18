package elog

import (
	"fmt"
	"os"
	"sync"
)

var mu sync.Mutex

func Println(x ...interface{}) {
	mu.Lock()
	defer mu.Unlock()

	fmt.Fprintln(os.Stderr, x...)
}

func Printf(f string, x ...interface{}) {
	mu.Lock()
	defer mu.Unlock()

	fmt.Fprintf(os.Stderr, f, x...)
}
