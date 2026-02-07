//go:build integration

package testfixtures

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
)

var testClient *elasticsearch.Client

func TestMain(m *testing.M) {
	addr := os.Getenv("ELASTICSEARCH_URL")
	if addr == "" {
		addr = "http://localhost:9200"
	}

	var err error
	testClient, err = elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{addr},
	})
	if err != nil {
		fmt.Printf("creating ES client: %v\n", err)
		os.Exit(1)
	}

	res, err := testClient.Ping()
	if err != nil {
		fmt.Printf("Elasticsearch not available: %v\n", err)
		os.Exit(1)
	}
	res.Body.Close()

	os.Exit(m.Run())
}

func setupTestClient(t *testing.T) *elasticsearch.Client {
	t.Helper()
	return testClient
}

// getDocCount returns the number of documents in the given index.
func getDocCount(t *testing.T, client *elasticsearch.Client, index string) int {
	t.Helper()

	res, err := client.Count(
		client.Count.WithIndex(index),
		client.Count.WithContext(context.Background()),
	)
	if err != nil {
		t.Fatalf("counting documents in %q: %v", index, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		t.Fatalf("counting documents in %q: %s", index, res.Status())
	}

	var result struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		t.Fatalf("decoding count response: %v", err)
	}

	return result.Count
}

// getDocument retrieves a document by ID from the given index.
func getDocument(t *testing.T, client *elasticsearch.Client, index, id string) map[string]interface{} {
	t.Helper()

	res, err := client.Get(index, id,
		client.Get.WithContext(context.Background()),
	)
	if err != nil {
		t.Fatalf("getting document %q from %q: %v", id, index, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		t.Fatalf("getting document %q from %q: %s", id, index, res.Status())
	}

	var result struct {
		Source map[string]interface{} `json:"_source"`
	}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		t.Fatalf("decoding get response: %v", err)
	}

	return result.Source
}

// getIndexMapping retrieves the mapping of the given index.
func getIndexMapping(t *testing.T, client *elasticsearch.Client, index string) map[string]interface{} {
	t.Helper()

	res, err := client.Indices.GetMapping(
		client.Indices.GetMapping.WithIndex(index),
		client.Indices.GetMapping.WithContext(context.Background()),
	)
	if err != nil {
		t.Fatalf("getting mapping for %q: %v", index, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		t.Fatalf("getting mapping for %q: %s", index, res.Status())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		t.Fatalf("decoding mapping response: %v", err)
	}

	return result
}

// indexExists checks whether the given index exists.
func indexExists(t *testing.T, client *elasticsearch.Client, index string) bool {
	t.Helper()

	res, err := client.Indices.Exists([]string{index},
		client.Indices.Exists.WithContext(context.Background()),
	)
	if err != nil {
		t.Fatalf("checking existence of %q: %v", index, err)
	}
	defer res.Body.Close()

	return !res.IsError()
}

func TestLoadAndClean_BasicRoundTrip(t *testing.T) {
	client := setupTestClient(t)

	loader, err := New(client, Directory("testdata/fixtures"))
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	// Load fixtures (deletes existing indices first)
	if err := loader.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	// Verify indices exist and have correct document counts
	if count := getDocCount(t, client, "users"); count != 2 {
		t.Errorf("expected 2 users documents, got %d", count)
	}

	if count := getDocCount(t, client, "products"); count != 3 {
		t.Errorf("expected 3 products documents, got %d", count)
	}

	// Clean up
	if err := loader.Clean(); err != nil {
		t.Fatalf("Clean() error: %v", err)
	}

	// Verify indices are deleted
	if indexExists(t, client, "users") {
		t.Error("users index should not exist after Clean()")
	}
	if indexExists(t, client, "products") {
		t.Error("products index should not exist after Clean()")
	}
}

func TestLoad_MappingApplied(t *testing.T) {
	client := setupTestClient(t)

	loader, err := New(client, Directory("testdata/fixtures"))
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if err := loader.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	t.Cleanup(func() { loader.Clean() })

	// Verify mapping for users index
	mapping := getIndexMapping(t, client, "users")
	usersMapping, ok := mapping["users"].(map[string]interface{})
	if !ok {
		t.Fatal("expected users index in mapping response")
	}

	mappings, ok := usersMapping["mappings"].(map[string]interface{})
	if !ok {
		t.Fatal("expected mappings in users index")
	}

	properties, ok := mappings["properties"].(map[string]interface{})
	if !ok {
		t.Fatal("expected properties in mappings")
	}

	emailProp, ok := properties["email"].(map[string]interface{})
	if !ok {
		t.Fatal("expected email property")
	}

	if emailType, ok := emailProp["type"].(string); !ok || emailType != "keyword" {
		t.Errorf("expected email type 'keyword', got %v", emailProp["type"])
	}
}

func TestLoad_DocumentIDs(t *testing.T) {
	client := setupTestClient(t)

	loader, err := New(client, Directory("testdata/fixtures"))
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if err := loader.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	t.Cleanup(func() { loader.Clean() })

	// Verify documents are retrievable by their explicit IDs
	doc := getDocument(t, client, "users", "1")
	if name, ok := doc["name"].(string); !ok || name != "Alice" {
		t.Errorf("expected name 'Alice', got %v", doc["name"])
	}

	doc = getDocument(t, client, "products", "p3")
	if title, ok := doc["title"].(string); !ok || title != "Go Programming" {
		t.Errorf("expected title 'Go Programming', got %v", doc["title"])
	}
}

func TestLoad_ReloadsCleanState(t *testing.T) {
	client := setupTestClient(t)

	loader, err := New(client, Directory("testdata/fixtures"))
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	// Load twice to verify clean reload
	if err := loader.Load(); err != nil {
		t.Fatalf("first Load() error: %v", err)
	}

	if err := loader.Load(); err != nil {
		t.Fatalf("second Load() error: %v", err)
	}
	t.Cleanup(func() { loader.Clean() })

	// Document count should be the same (not doubled)
	if count := getDocCount(t, client, "users"); count != 2 {
		t.Errorf("expected 2 users documents after reload, got %d", count)
	}
}

func TestClean_Idempotent(t *testing.T) {
	client := setupTestClient(t)

	loader, err := New(client, Directory("testdata/fixtures"))
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if err := loader.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	// Clean twice should not error
	if err := loader.Clean(); err != nil {
		t.Fatalf("first Clean() error: %v", err)
	}

	if err := loader.Clean(); err != nil {
		t.Fatalf("second Clean() error: %v", err)
	}
}

func TestNew_NilClient(t *testing.T) {
	_, err := New(nil, Directory("testdata/fixtures"))
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestNew_MissingDirectory(t *testing.T) {
	client := setupTestClient(t)

	_, err := New(client)
	if err == nil {
		t.Fatal("expected error for missing Directory option")
	}
}

func TestNew_NonExistentDirectory(t *testing.T) {
	client := setupTestClient(t)

	_, err := New(client, Directory("/nonexistent/path"))
	if err == nil {
		t.Fatal("expected error for non-existent directory")
	}
}

func TestLoad_NoMappingOrSettings(t *testing.T) {
	client := setupTestClient(t)

	// Create a temp fixture dir with no mapping/settings
	dir := t.TempDir()
	indexDir := fmt.Sprintf("%s/dynamic_index", dir)
	if err := os.Mkdir(indexDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fmt.Sprintf("%s/documents.yml", indexDir), []byte("- name: test\n  value: hello\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	loader, err := New(client, Directory(dir))
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if err := loader.Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	t.Cleanup(func() { loader.Clean() })

	if count := getDocCount(t, client, "dynamic_index"); count != 1 {
		t.Errorf("expected 1 document, got %d", count)
	}
}
