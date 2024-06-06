package main

import (
	"fmt"
	"os"
	"parcompress/execution"
	"strconv"
	"time"
)

const usage = "Usage: process data_dir mode [number of threads] worksteal\n" +
	"data_dir = The data directory to use to compress the data.\n" +
	"mode     = (s) run sequentially, (p) run in parallel \n" +
	"[number of threads] = Runs the parallel version of the program with the specified number of threads.\n" +
	"worksteal = (true) run worksteal, (false) run without worksteal\n"

func main() {
	start := time.Now()
	// Set default values
	mode := "s"
	dataDir := "small"
	workSteal := false
	threadCount := 1
	if len(os.Args) < 2 || len(os.Args) > 5 {
		// Print usage if the number of arguments is less than 2
		fmt.Println(usage)
		return
	} else {
		// Set the data directory
		dataDir = os.Args[1]
		// Check if the data directory is valid
		if dataDir != "small" && dataDir != "medium" && dataDir != "large" {
			fmt.Println(usage)
			return
		}
		if len(os.Args) == 3 {
			// Check if the mode is sequential
			mode = os.Args[2]
			if mode != "s" {
				fmt.Println(usage)
				return
			}
		}
		if len(os.Args) >= 4 {
			// Check if the mode is parallel
			mode = os.Args[2]
			if mode != "p" {
				fmt.Println(usage)
				return
			}
			// Check if the thread count is valid
			var err error
			threadCount, err = strconv.Atoi(os.Args[3])
			if threadCount <= 1 || err != nil {
				fmt.Println(usage)
				return
			}
			if len(os.Args) == 5 {
				// Check if worksteal is enabled
				workSteal, err = strconv.ParseBool(os.Args[4])
				if err != nil {
					fmt.Println(usage)
					return
				}
			}
		}
	}
	// Run the program
	if mode == "s" {
		execution.Sequential(dataDir)
	} else {
		if workSteal {
			execution.ParallelWorkSteal(dataDir, threadCount)
		} else {
			execution.Parallel(dataDir, threadCount)
		}
	}
	// Print the time taken to run the program
	fmt.Printf("%.2f\n", time.Since(start).Seconds())
}
