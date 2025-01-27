package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/joho/godotenv"
)

// Column represents column metadata
type Column struct {
	Name       string
	Types      []string
	ClickHType string
}

// determineClickHouseType maps detected types to ClickHouse types
func determineClickHouseType(types []string) string {
	if containsNull(types) {
		if contains(types, "number") {
			return "Nullable(Float64)"
		}
		if contains(types, "string") {
			return "Nullable(String)"
		}
	}

	if contains(types, "number") {
		return "Float64"
	}
	return "String"
}

// contains checks if slice contains an element
func contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

// containsNull checks if slice contains null
func containsNull(slice []string) bool {
	return contains(slice, "null")
}

// analyzeColumnTypes detects column types from JSON
func analyzeColumnTypes(filePath string) ([]Column, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var jsonData []map[string]interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, err
	}

	columnTypes := make(map[string]map[string]bool)
	for _, row := range jsonData {
		for column, value := range row {
			if columnTypes[column] == nil {
				columnTypes[column] = make(map[string]bool)
			}

			var typeKey string
			switch value.(type) {
			case nil:
				typeKey = "null"
			case float64:
				typeKey = "number"
			case string:
				typeKey = "string"
			default:
				typeKey = "unknown"
			}
			columnTypes[column][typeKey] = true
		}
	}

	var columns []Column
	for column, types := range columnTypes {
		typeList := make([]string, 0, len(types))
		for t := range types {
			typeList = append(typeList, t)
		}
		columns = append(columns, Column{
			Name:       sanitizeColumnName(column),
			Types:      typeList,
			ClickHType: determineClickHouseType(typeList),
		})
	}

	return columns, nil
}

// sanitizeColumnName removes spaces and special characters
func sanitizeColumnName(name string) string {
	// Replace spaces with underscores
	name = strings.ReplaceAll(name, " ", "_")

	// Remove any non-alphanumeric characters except underscores
	var result []rune
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			result = append(result, r)
		}
	}

	return string(result)
}

// createTable generates and executes CREATE TABLE statement
func createTable(conn driver.Conn, tableName string, columns []Column) error {
	var createTableQuery []string
	for _, col := range columns {
		createTableQuery = append(createTableQuery, fmt.Sprintf("`%s` %s", col.Name, col.ClickHType))
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (%s) ENGINE = MergeTree() ORDER BY tuple()",
		tableName, strings.Join(createTableQuery, ", "))

	fmt.Println("CREATE TABLE Query:", query)
	return conn.Exec(context.Background(), query)
}

// bulkInsert performs batch insert of JSON data
func bulkInsert(conn driver.Conn, tableName string, columns []Column, filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	var jsonData []map[string]interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return err
	}

	batch, err := conn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO `%s`", tableName))
	if err != nil {
		return err
	}

	for _, row := range jsonData {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			val := row[strings.ReplaceAll(col.Name, "_", " ")]

			// Handle conversion and nullability
			if val == nil {
				values[i] = nil
				continue
			}

			switch col.ClickHType {
			case "Float64", "Nullable(Float64)":
				values[i] = convertToFloat(val)
			default:
				values[i] = fmt.Sprintf("%v", val)
			}
		}

		if err := batch.Append(values...); err != nil {
			return err
		}
	}

	return batch.Send()
}

// convertToFloat handles various numeric conversions
func convertToFloat(val interface{}) float64 {
	switch v := val.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case string:
		f, _ := strconv.ParseFloat(v, 64)
		return f
	default:
		return 0
	}
}

func main() {
	// ClickHouse connection parameters
	er := godotenv.Load(".env")
	if er != nil {
		log.Fatal("Error loading .env file:", er)
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "mysql_user",
			Password: os.Getenv("PASSWORD"),
		},
	})
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	tableName := "loan_data"
	filePath := "output.json"

	// Analyze column types
	columns, err := analyzeColumnTypes(filePath)
	if err != nil {
		panic(err)
	}

	// Print column types for verification
	for _, col := range columns {
		fmt.Printf("%s: %v (ClickHouse Type: %s)\n", col.Name, col.Types, col.ClickHType)
	}

	// Create table
	if err := createTable(conn, tableName, columns); err != nil {
		panic(err)
	}

	// Bulk insert
	if err := bulkInsert(conn, tableName, columns, filePath); err != nil {
		panic(err)
	}

	fmt.Println("Data successfully imported to ClickHouse")
}
