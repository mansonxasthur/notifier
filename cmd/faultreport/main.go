// Command faultreport reads the result directory of a fault run (see loadtest/faults.sh) and
// writes fault-report.md beside it: when the fault was injected and released, the queue and
// breaker traces around it, how long the system took to recover, and how many deliveries the
// simulator received more than once.
package main

import (
	"fmt"
	"os"

	"github.com/mansonxasthur/notifier/internal/loadtest"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: faultreport <result directory>")
		os.Exit(2)
	}
	if err := loadtest.WriteFaultReport(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, "faultreport:", err)
		os.Exit(1)
	}
}
