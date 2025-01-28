package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/joho/godotenv"
)

type Column struct {
	Name       string
	ClickHType string
}

// detectDataType determines the ClickHouse data type based on a sample value
func detectDataType(value string) string {
	if value == "" {
		return "Nullable(String)"
	}

	// Try to parse as integer
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return "Nullable(Int32)"
	}

	// Try to parse as float
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return "Nullable(Float64)"
	}

	// Default to String for any other type
	return "Nullable(String)"
}

// convertValue converts a string value to the appropriate type based on ClickHouse type
func convertValue(value string, clickhType string) interface{} {
	if value == "" {
		return nil
	}

	switch clickhType {
	case "Nullable(Int32)":
		if val, err := strconv.ParseInt(value, 10, 32); err == nil {
			return int32(val)
		}
		return nil
	case "Nullable(Float64)":
		if val, err := strconv.ParseFloat(value, 64); err == nil {
			return val
		}
		return nil
	default:
		return value
	}
}

func main() {
	er := godotenv.Load(".env")
	if er != nil {
		log.Fatal("Error loading .env file:", er)
	}
	// Open CSV file
	file, err := os.Open("BankCustomerData.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)

	// Read header
	headers, err := reader.Read()
	if err != nil {
		panic(err)
	}

	// Read first row to determine data types
	firstRow, err := reader.Read()
	if err != nil {
		panic(err)
	}

	// Create columns slice
	columns := make([]Column, len(headers))
	for i, header := range headers {
		columns[i] = Column{
			Name:       header,
			ClickHType: detectDataType(firstRow[i]),
		}
	}

	// Create connection to ClickHouse
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
	})
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Create table query
	createTableQuery := "CREATE TABLE IF NOT EXISTS bank_customer_data (\n"
	for _, col := range columns {
		createTableQuery += fmt.Sprintf("    `%s` %s,\n", col.Name, col.ClickHType)
	}
	createTableQuery = strings.TrimSuffix(createTableQuery, ",\n") + "\n) ENGINE = MergeTree() ORDER BY tuple()"

	// Execute create table query
	err = conn.Exec(context.Background(), createTableQuery)
	if err != nil {
		panic(err)
	}
	fmt.Println("Table created successfully")

	// Reset file pointer and skip header
	file.Seek(0, 0)
	reader = csv.NewReader(file)
	reader.Read() // Skip header

	// Prepare batch insert
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO bank_customer_data")
	if err != nil {
		panic(err)
	}

	// Read and insert data
	rowCount := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		// Convert record to proper types
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = convertValue(record[i], col.ClickHType)
		}

		// Append to batch
		if err := batch.Append(values...); err != nil {
			panic(err)
		}

		rowCount++
		if rowCount%1000 == 0 {
			fmt.Printf("Processed %d rows...\n", rowCount)
		}
	}

	// Send the batch
	if err := batch.Send(); err != nil {
		panic(err)
	}

	fmt.Printf("Data import completed successfully. Total rows processed: %d\n", rowCount)
}
