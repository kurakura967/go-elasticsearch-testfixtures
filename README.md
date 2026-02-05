# go-elasticsearch-testfixtures

A Go library for loading Elasticsearch test fixtures in integration tests.
Manage index mappings, settings, and test documents through a simple directory structure.

Inspired by [go-testfixtures/testfixtures](https://github.com/go-testfixtures/testfixtures).

## Installation

```bash
go get github.com/kurakura967/go-elasticsearch-testfixtures
```

## Fixture Directory Structure

Organize your test data as directories and files:

```
testdata/fixtures/
├── users/
│   ├── _mapping.json       # Index mapping (optional)
│   ├── _settings.json      # Index settings (optional)
│   └── documents.yml       # Test documents
└── products/
    ├── _mapping.json
    ├── 001_electronics.yml  # Multiple document files supported
    └── 002_books.yml
```

- Each subdirectory represents an Elasticsearch index
- `_mapping.json` defines the index mapping (same format as the ES Mappings API)
- `_settings.json` defines the index settings (same format as the ES Settings API)
- `*.yml` / `*.yaml` files (not starting with `_`) contain test documents

### _mapping.json

```json
{
  "properties": {
    "name": { "type": "text" },
    "email": { "type": "keyword" }
  }
}
```

### _settings.json

```json
{
  "number_of_shards": 1,
  "number_of_replicas": 0
}
```

### documents.yml

```yaml
- _id: "1"
  name: "Alice"
  email: "alice@example.com"

- _id: "2"
  name: "Bob"
  email: "bob@example.com"
```

The `_id` field is optional. If provided, it is used as the Elasticsearch document ID and removed from the document body. If omitted, Elasticsearch auto-generates the ID.

## Usage

```go
package myapp_test

import (
	"log"
	"os"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	testfixtures "github.com/kurakura967/go-elasticsearch-testfixtures"
)

var fixtures *testfixtures.Loader

func TestMain(m *testing.M) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
	})
	if err != nil {
		log.Fatal(err)
	}

	fixtures, err = testfixtures.New(
		client,
		testfixtures.Directory("testdata/fixtures"),
	)
	if err != nil {
		log.Fatal(err)
	}

	os.Exit(m.Run())
}

func TestSomething(t *testing.T) {
	// Load fixtures: deletes existing indices, recreates them, and inserts documents
	if err := fixtures.Load(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fixtures.Clean() })

	// Your test code here...
}
```

## API

### `New(client, opts...) (*Loader, error)`

Creates a new Loader. Requires an `*elasticsearch.Client` and the `Directory` option.

### `(*Loader).Load() error`

Deletes existing indices, recreates them with mappings/settings, inserts documents, and refreshes indices so documents are immediately searchable.

### `(*Loader).Clean() error`

Deletes all indices managed by this Loader.

### Options

| Option | Description |
|--------|-------------|
| `Directory(path)` | Path to the fixtures directory (required) |
| `WithContext(ctx)` | Default context for ES operations (default: `context.Background()`) |

## Running Tests

```bash
# Unit tests
go test ./...

# Integration tests (requires Elasticsearch)
docker compose up -d
go test -v -tags integration ./...
```

## License

MIT
