package testfixtures

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	mappingFile  = "_mapping.json"
	settingsFile = "_settings.json"
)

// parseFixtures scans the fixtures directory and parses all index subdirectories.
func parseFixtures(dir string) ([]*indexFixture, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading fixtures directory %q: %w", dir, err)
	}

	var fixtures []*indexFixture
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		f, err := parseIndexDir(filepath.Join(dir, entry.Name()), entry.Name())
		if err != nil {
			return nil, fmt.Errorf("parsing index %q: %w", entry.Name(), err)
		}
		fixtures = append(fixtures, f)
	}

	if len(fixtures) == 0 {
		return nil, fmt.Errorf("no index directories found in %q", dir)
	}

	return fixtures, nil
}

// parseIndexDir parses a single index directory containing schema and document files.
func parseIndexDir(dir string, name string) (*indexFixture, error) {
	f := &indexFixture{name: name}

	mapping, err := readJSONFile(filepath.Join(dir, mappingFile))
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("reading %s: %w", mappingFile, err)
	}
	f.mapping = mapping

	settings, err := readJSONFile(filepath.Join(dir, settingsFile))
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("reading %s: %w", settingsFile, err)
	}
	f.settings = settings

	docs, err := parseDocumentFiles(dir)
	if err != nil {
		return nil, err
	}
	f.documents = docs

	return f, nil
}

// readJSONFile reads a JSON file and returns its content as json.RawMessage.
// Returns nil, nil if the file does not exist (os.IsNotExist).
func readJSONFile(path string) (json.RawMessage, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if !json.Valid(data) {
		return nil, fmt.Errorf("invalid JSON in %q", path)
	}

	return json.RawMessage(data), nil
}

// parseDocumentFiles finds and parses all YAML document files in the directory.
// Document files are *.yml files that do not start with "_".
func parseDocumentFiles(dir string) ([]document, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading directory %q: %w", dir, err)
	}

	var docs []document
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".yml") && !strings.HasSuffix(name, ".yaml") {
			continue
		}
		if strings.HasPrefix(name, "_") {
			continue
		}

		fileDocs, err := parseYAMLDocuments(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("parsing document file %q: %w", name, err)
		}
		docs = append(docs, fileDocs...)
	}

	return docs, nil
}

// parseYAMLDocuments parses a YAML file containing an array of documents.
func parseYAMLDocuments(path string) ([]document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	var rawDocs []map[string]interface{}
	if err := yaml.Unmarshal(data, &rawDocs); err != nil {
		return nil, fmt.Errorf("unmarshaling YAML: %w", err)
	}

	docs := make([]document, 0, len(rawDocs))
	for _, raw := range rawDocs {
		doc := document{
			Body: raw,
		}

		if id, ok := raw["_id"]; ok {
			doc.ID = fmt.Sprintf("%v", id)
			delete(doc.Body, "_id")
		}

		docs = append(docs, doc)
	}

	return docs, nil
}
