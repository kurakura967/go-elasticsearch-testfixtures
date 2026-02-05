package testfixtures

import "encoding/json"

// indexFixture represents a single Elasticsearch index and its fixture data.
type indexFixture struct {
	name      string            // Directory name = index name
	mapping   json.RawMessage   // Contents of _mapping.json (may be nil)
	settings  json.RawMessage   // Contents of _settings.json (may be nil)
	documents []document        // Parsed documents from YAML files
}

// document represents a single Elasticsearch document to be indexed.
type document struct {
	ID   string                 // Extracted from _id field (may be empty for auto-generated IDs)
	Body map[string]interface{} // Document body (without _id)
}
