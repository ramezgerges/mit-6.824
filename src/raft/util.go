package raft

import (
	"log"
	"os"
)

func DPrintf(format string, a ...interface{}) {
	log.SetFlags(log.Lmicroseconds | log.Ltime)
	if os.Getenv("DEBUG") == "1" {
		log.Printf(format, a...)
	}
	return
}

func CompareEntries(term1, index1, term2, index2 int) int {
	if term1-term2 != 0 {
		return term1 - term2
	} else {
		return index1 - index2
	}
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a < b {
		return b
	}
	return a
}
