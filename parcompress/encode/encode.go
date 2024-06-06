package encode

import (
	"bufio"
	"encoding/gob"
	"log"
	"math/rand"
	"os"
	"parcompress/workSteal"
	"regexp"
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

// Struct to hold the token and its index
type TokenItem struct {
	index int
	token string
}

// Generate tokens from the file
func GenerateTokens(filePath string) []string {
	tokens := []string{}
	// Open the file
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Read the file line by line
	scanner := bufio.NewScanner(f)
	// Define a regular expression to split the line into tokens
	// The regular expression matches words, hyphens, apostrophes, periods, and double quotes
	// Split when a space, comma, or newline is encountered
	re := regexp.MustCompile(`[\w-.'"]+|[ ,\n]`)
	for scanner.Scan() {
		line := scanner.Text()
		// Split the line into tokens using the regular expression
		splittedTokens := re.FindAllString(line, -1)
		// Append the tokens to the list of tokens
		tokens = append(tokens, splittedTokens...)
		tokens = append(tokens, "\\n")
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return tokens
}

// Generate codebook of unique tokens for encoding and decoding sequentially
func GenerateCodes(tokens []string) []string {
	tokenMap := make(map[string]bool)
	// Create a map of tokens to remove duplicates
	for _, token := range tokens {
		tokenMap[token] = true
	}

	codebook := []string{}
	// Append the unique tokens to the codebook
	for token := range tokenMap {
		codebook = append(codebook, token)
	}

	return codebook
}

// Generate codebook of unique tokens for encoding and decoding in parallel
func ParallelGenerateCodes(tokens []string, threadCount int) []string {
	// Create a channel to send tokens to the secondary workers
	channel := make(chan string, len(tokens))
	// Create secondary workers to append tokens to the codebook
	secondaryWorkers := threadCount - 1
	codebook := []string{}
	// Create a wait group to wait for the secondary workers to finish
	done := make(chan bool, secondaryWorkers)
	var mu sync.Mutex

	// Spawn secondary workers to append tokens to the codebook
	for i := 0; i < secondaryWorkers; i++ {
		go func() {
			for {
				// While there are more tokens in the channel, append the token to the codebook
				token, more := <-channel
				// If there are no more tokens, i.e. channel is closed, more is false
				if more {
					// Ensure there is no race condition when appending to the codebook
					mu.Lock()
					codebook = append(codebook, token)
					mu.Unlock()
				} else {
					done <- true
					return
				}
			}
		}()
	}

	tokenMap := make(map[string]bool)
	for _, token := range tokens {
		// If the token is not in the map, send it to the channel for
		// the secondary workers to append to the codebook
		_, ok := tokenMap[token]
		if !ok {
			channel <- token
		}
		// Add the token to the map to avoid duplicates
		tokenMap[token] = true
	}
	// Close the channel to signal the secondary workers to finish
	close(channel)

	// Wait for the secondary workers to finish
	for i := 0; i < secondaryWorkers; i++ {
		<-done
	}

	return codebook
}

// Encode the tokens using the codebook sequentially
func Encode(tokens []string, codes []string) []int {
	encodedList := []int{}
	for _, token := range tokens {
		// Look up the token's index in the codebook and append the index to the encoded list
		for index, code := range codes {
			if code == token {
				encodedList = append(encodedList, index)
				break
			}
		}
	}
	return encodedList
}

// Encode the tokens using the codebook in parallel
func ParallelEncode(tokens []string, codes []string, threadCount int) []int {
	var wg sync.WaitGroup
	var mutex sync.Mutex
	condVar := sync.NewCond(&mutex)
	// Create shared context for threads to communicate
	context := SharedContext{wgContext: &wg, threadCount: threadCount, cond: condVar, mutex: &mutex}
	partSize := len(tokens) / threadCount
	encodedList := make([]int, len(tokens))
	// Spawn threads to begin encoding, dividing the tokens into parts
	for i := 0; i < threadCount; i++ {
		start := i * partSize
		end := start + partSize
		if i == threadCount-1 {
			end = len(tokens)
		}
		wg.Add(1)
		go EncodeWorker(i, &context, tokens[start:end], codes, encodedList[start:end])
	}
	wg.Wait()
	return encodedList
}

// Worker function to encode a portion of the tokens
func EncodeWorker(goID int, ctx *SharedContext, tokens []string, codes []string, encodedList []int) {
	// Encode the tokens using the codebook
	for i, token := range tokens {
		for index, code := range codes {
			if code == token {
				encodedList[i] = index
				break
			}
		}
	}

	// When done encoding, add to the counter to signal completion
	ctx.mutex.Lock()
	ctx.counter++
	if ctx.counter == ctx.threadCount {
		// If this is the last thread to finish, broadcast to all threads to wake up and wrap up
		ctx.cond.Broadcast()
	} else {
		for ctx.counter != ctx.threadCount {
			// Wait for all threads to finish encoding
			ctx.cond.Wait()
		}
	}
	ctx.mutex.Unlock()

	ctx.wgContext.Done()
}

// Encode the tokens using the codebook in parallel with work stealing
func ParallelWorkStealEncode(tokens []string, codes []string, threadCount int) []int {
	var wg sync.WaitGroup
	var mutex sync.Mutex
	condVar := sync.NewCond(&mutex)
	// Create shared context for threads to communicate
	context := SharedContext{wgContext: &wg, threadCount: threadCount, cond: condVar, mutex: &mutex}
	encodedList := make([]int, len(tokens))
	// Create a slice of work queues for each thread
	workQueues := make([]workSteal.DEQueue, threadCount)
	partSize := len(tokens) / threadCount
	// Spawn threads to begin encoding with work stealing, dividing the tokens into parts
	for i := 0; i < threadCount; i++ {
		start := i * partSize
		end := start + partSize
		if i == threadCount-1 {
			end = len(tokens)
		}
		wg.Add(1)
		go EncodeWorkStealWorker(i, &context, tokens[start:end], codes, encodedList, workQueues, start, end, threadCount)
	}
	wg.Wait()
	return encodedList
}

// Worker function to encode a portion of the tokens with work stealing
func EncodeWorkStealWorker(goID int, ctx *SharedContext, tokens []string, codes []string, encodedList []int, workQueues []workSteal.DEQueue, start int, end int, threadCount int) {
	tokenSize := len(tokens)
	// Initialize a new work queue for the current thread
	workQueues[goID] = *workSteal.NewDEQueue(uint64(tokenSize))
	for i, token := range tokens {
		// Push the token and its index to the work queue
		item := TokenItem{index: i + start, token: token}
		workQueues[goID].PushBottom(item)
	}
	for {
		// Pop the token from the bottom of the work queue
		poppedBottomItem := workQueues[goID].PopBottom()
		if poppedBottomItem != nil {
			// Encode the token using the codebook
			returnedItem := poppedBottomItem.(TokenItem)
			for index, code := range codes {
				if code == returnedItem.token {
					encodedList[returnedItem.index] = index
					break
				}
			}
		} else {
			// If there is no more work in the work queue, break
			break
		}
	}
	// Increment the counter to signal completion
	ctx.mutex.Lock()
	ctx.counter++
	ctx.mutex.Unlock()
	// Create a list of other threads to steal work from
	otherThreads := []int{}
	for i := 0; i < ctx.threadCount; i++ {
		if i != goID {
			otherThreads = append(otherThreads, i)
		}
	}
	// While the counter is not equal to the thread count, other threads have not finished
	for ctx.counter != ctx.threadCount {
		// Steal work from other threads randomly
		randomIndex := rand.Intn(len(otherThreads))
		otherThreadID := otherThreads[randomIndex]
		// Pop the token from the top of the work queue of the other thread
		poppedTopItem := workQueues[otherThreadID].PopTop()
		if poppedTopItem != nil {
			// Encode the token using the codebook
			stolenItem := poppedTopItem.(TokenItem)
			for index, code := range codes {
				if code == stolenItem.token {
					encodedList[stolenItem.index] = index
					break
				}
			}
		}
	}
	// When all threads have finished, decrement the counter and signal completion
	ctx.wgContext.Done()
}

// Serialize the encoded list and codebook to a file
func Serialize(outputFile string, encodedList []int, codeBook []string) int64 {
	// Create a struct to hold the compressed data
	data := CompressedData{
		EncodedList: encodedList,
		CodeBook:    codeBook,
	}

	// Open the file for writing
	file, err := os.Create(outputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Use an encoder to encode the data and store them as binary values to compress the data
	enc := gob.NewEncoder(file)
	if err := enc.Encode(data); err != nil {
		log.Fatal(err)
	}

	// Return the size of the file
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}
	return fileInfo.Size()
}
