package decode

import (
	"bufio"
	"encoding/gob"
	"log"
	"os"
	"sync"
)

// Struct to hold the compressed data for serialization and deserialization
type CompressedData struct {
	EncodedList []int
	CodeBook    []string
}

// Struct to hold shared context between threads in BSP model
type SharedContext struct {
	mutex       *sync.Mutex
	cond        *sync.Cond
	wgContext   *sync.WaitGroup
	counter     int
	threadCount int
}

// Sequentially decode the encoded list of integers using the codebook
func Decode(encoded []int, codes []string) []string {
	originalTokens := []string{}
	for _, code := range encoded {
		// Decode the encoded list of integers using the codebook and
		// append the original tokens to the originalTokens list
		originalTokens = append(originalTokens, codes[code])
	}
	return originalTokens
}

// Decode the encoded list of integers using the codebook in parallel
func ParallelDecode(encoded []int, codes []string, threadCount int) []string {
	var wg sync.WaitGroup
	var mutex sync.Mutex
	condVar := sync.NewCond(&mutex)
	// Create shared context for threads to communicate
	context := SharedContext{wgContext: &wg, threadCount: threadCount, cond: condVar, mutex: &mutex}
	partSize := len(encoded) / threadCount
	originalTokens := make([]string, len(encoded))
	// Spawn threads to begin decoding, dividing the encoded list into parts
	for i := 0; i < threadCount; i++ {
		start := i * partSize
		end := start + partSize
		if i == threadCount-1 {
			end = len(encoded)
		}
		wg.Add(1)
		go DecodeWorker(i, &context, encoded[start:end], codes, originalTokens[start:end])
	}
	wg.Wait()
	return originalTokens
}

// Worker function to decode a portion of the encoded list
func DecodeWorker(goID int, ctx *SharedContext, encoded []int, codes []string, originalTokens []string) {
	// Decode the encoded list of integers using the codebook
	for i, code := range encoded {
		originalTokens[i] = codes[code]
	}

	// When done decoding, add to the counter to signal completion
	ctx.mutex.Lock()
	ctx.counter++
	if ctx.counter == ctx.threadCount {
		// If this is the last thread to finish, broadcast to all threads to wake up and wrap up
		ctx.cond.Broadcast()
	} else {
		for ctx.counter != ctx.threadCount {
			// Wait for all threads to finish decoding
			ctx.cond.Wait()
		}
	}
	ctx.mutex.Unlock()

	ctx.wgContext.Done()
}

// Deserialize the compressed data from a file
func Deserialize(compressedFile string) ([]int, []string) {
	// Open the compressed file
	file, err := os.Open(compressedFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Use a decoder to decode the compressed data stored as binary values
	dec := gob.NewDecoder(file)
	// Use a struct to hold the decoded compressed data
	data := CompressedData{}
	if err := dec.Decode(&data); err != nil {
		log.Fatal(err)
	}

	// Return the decoded list of integers and the codebook
	return data.EncodedList, data.CodeBook
}

// Output the decoded list of tokens to a file
func OutputToFile(decodedList []string, filename string) {
	// Create a file to write the decoded list of tokens
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Write the decoded list of tokens to the file
	w := bufio.NewWriter(f)
	for _, token := range decodedList {
		if token == "\\n" {
			// Write a newline character if the token is a newline
			_, err := w.WriteString("\n")
			if err != nil {
				log.Fatal(err)
			}
		} else {
			// Else, write the token to the file
			_, err := w.WriteString(token)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	err = w.Flush()
	if err != nil {
		log.Fatal(err)
	}
}
