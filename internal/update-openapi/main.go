// Command update-openapi regenerates testdata/openapi-operations.json from a
// running Hot API server's OpenAPI spec (or a local spec file).
//
//	go run ./internal/update-openapi [url-or-path]
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
)

const defaultSource = "http://localhost:4681/openapi.json"

type operation struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

func main() {
	source := defaultSource
	if len(os.Args) > 1 {
		source = os.Args[1]
	}

	spec, err := loadSpec(source)
	if err != nil {
		log.Fatal(err)
	}
	paths, ok := spec["paths"].(map[string]any)
	if !ok {
		log.Fatalf("no OpenAPI paths found in %s", source)
	}

	var operations []operation
	for path, item := range paths {
		pathItem, ok := item.(map[string]any)
		if !ok {
			continue
		}
		for _, method := range []string{"get", "post", "put", "patch", "delete"} {
			if _, ok := pathItem[method]; ok {
				operations = append(operations, operation{
					Method: strings.ToUpper(method),
					Path:   path,
				})
			}
		}
	}

	sort.Slice(operations, func(i, j int) bool {
		if operations[i].Path != operations[j].Path {
			return operations[i].Path < operations[j].Path
		}
		return operations[i].Method < operations[j].Method
	})

	encoded, err := json.MarshalIndent(operations, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile("testdata/openapi-operations.json", append(encoded, '\n'), 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("wrote %d operations from %s\n", len(operations), source)
}

func loadSpec(source string) (map[string]any, error) {
	var payload []byte
	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		resp, err := http.Get(source)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("failed to fetch %s: %d", source, resp.StatusCode)
		}
		var spec map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&spec); err != nil {
			return nil, err
		}
		return spec, nil
	}

	payload, err := os.ReadFile(source)
	if err != nil {
		return nil, err
	}
	var spec map[string]any
	if err := json.Unmarshal(payload, &spec); err != nil {
		return nil, err
	}
	return spec, nil
}
