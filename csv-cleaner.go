package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

func main() {
	inputFile := "credit_train.csv"
	outputFile := "output.csv"

	// Open input file
	input, err := os.Open(inputFile)
	if err != nil {
		fmt.Printf("Error opening input file: %v\n", err)
		return
	}
	defer input.Close()

	// Create output file
	output, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer output.Close()

	// Create CSV readers and writers
	reader := csv.NewReader(input)
	writer := csv.NewWriter(output)
	defer writer.Flush()

	// Read and write header row first
	header, err := reader.Read()
	if err != nil {
		fmt.Printf("Error reading header: %v\n", err)
		return
	}
	if err := writer.Write(header); err != nil {
		fmt.Printf("Error writing header: %v\n", err)
		return
	}

	// Process CSV rows
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Error reading record: %v\n", err)
			continue
		}

		// Skip rows with empty primary key (first column)
		if strings.TrimSpace(record[0]) == "" {
			continue
		}

		// Replace NA with NULL
		for i := range record {
			if strings.ToUpper(record[i]) == "NA" {
				record[i] = ""
			}
		}

		// Process 13th column (index 12) for numeric conversion
		if len(record) > 12 {
			// Remove quotes if present
			cleanValue := strings.Trim(record[12], "\"")

			// Try converting to number
			_, err := strconv.ParseFloat(cleanValue, 64)
			if err != nil {
				// If conversion fails, set to empty (NULL)
				record[12] = ""
			}
		}

		// Write processed record
		if err := writer.Write(record); err != nil {
			fmt.Printf("Error writing record: %v\n", err)
		}
	}

	fmt.Println("CSV file processed successfully")
}
