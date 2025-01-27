package main

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
)

func analyzeColumnTypes(filePath string) (map[string][]string, error) {
	// Read JSON file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	// Unmarshal JSON into slice of maps
	var jsonData []map[string]interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, err
	}

	// Analyze column types
	columnTypes := make(map[string]map[string]bool)

	for _, row := range jsonData {
		for column, value := range row {
			if columnTypes[column] == nil {
				columnTypes[column] = make(map[string]bool)
			}

			// Determine type
			var typeKey string
			switch v := value.(type) {
			case nil:
				typeKey = "null"
			case float64:
				typeKey = "number"
			case string:
				typeKey = "string"
			default:
				typeKey = reflect.TypeOf(v).Name()
			}

			columnTypes[column][typeKey] = true
		}
	}

	// Convert to slice of types
	result := make(map[string][]string)
	for column, types := range columnTypes {
		typeList := make([]string, 0, len(types))
		for t := range types {
			typeList = append(typeList, t)
		}
		result[column] = typeList
	}

	return result, nil
}

func main() {
	filePath := "output.json"
	types, err := analyzeColumnTypes(filePath)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	for column, typeList := range types {
		fmt.Printf("%s: %v\n", column, typeList)
	}
}
