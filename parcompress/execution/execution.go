package execution

import (
	"fmt"
	"parcompress/decode"
	"parcompress/encode"
)

// Run the sequential version of the program
func Sequential(dataDir string) {
	// Set paths for IO operations
	inPath := fmt.Sprintf("../data/%s/data_%s.txt", dataDir, dataDir)
	compressedPath := fmt.Sprintf("../data/%s/data_%s.gob", dataDir, dataDir)
	outPath := fmt.Sprintf("../data/%s/data_%s_out.txt", dataDir, dataDir)
	// Encoding
	tokens := encode.GenerateTokens(inPath)
	codeBook := encode.GenerateCodes(tokens)
	encodedList := encode.Encode(tokens, codeBook)
	encode.Serialize(compressedPath, encodedList, codeBook)
	// Decoding
	encodedList, codeBook = decode.Deserialize(compressedPath)
	decodedList := decode.Decode(encodedList, codeBook)
	decode.OutputToFile(decodedList, outPath)
}

// Run the parallel version of the program
func Parallel(dataDir string, threadCount int) {
	// Set paths for IO operations
	inPath := fmt.Sprintf("../data/%s/data_%s.txt", dataDir, dataDir)
	compressedPath := fmt.Sprintf("../data/%s/data_%s.gob", dataDir, dataDir)
	outPath := fmt.Sprintf("../data/%s/data_%s_out.txt", dataDir, dataDir)
	// Encoding
	tokens := encode.GenerateTokens(inPath)
	codeBook := encode.ParallelGenerateCodes(tokens, threadCount)
	encodedList := encode.ParallelEncode(tokens, codeBook, threadCount)
	encode.Serialize(compressedPath, encodedList, codeBook)
	// Decoding
	encodedList, codeBook = decode.Deserialize(compressedPath)
	decodedList := decode.ParallelDecode(encodedList, codeBook, threadCount)
	decode.OutputToFile(decodedList, outPath)
}

// Run the worksteal version of the program
func ParallelWorkSteal(dataDir string, threadCount int) {
	// Set paths for IO operations
	inPath := fmt.Sprintf("../data/%s/data_%s.txt", dataDir, dataDir)
	compressedPath := fmt.Sprintf("../data/%s/data_%s.gob", dataDir, dataDir)
	outPath := fmt.Sprintf("../data/%s/data_%s_out.txt", dataDir, dataDir)
	// Encoding
	tokens := encode.GenerateTokens(inPath)
	codeBook := encode.ParallelGenerateCodes(tokens, threadCount)
	encodedList := encode.ParallelWorkStealEncode(tokens, codeBook, threadCount)
	encode.Serialize(compressedPath, encodedList, codeBook)
	// Decoding
	encodedList, codeBook = decode.Deserialize(compressedPath)
	decodedList := decode.ParallelDecode(encodedList, codeBook, threadCount)
	decode.OutputToFile(decodedList, outPath)
}
