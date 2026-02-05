package testfixtures

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseFixtures(t *testing.T) {
	fixtures, err := parseFixtures("testdata/fixtures")
	if err != nil {
		t.Fatalf("parseFixtures() error: %v", err)
	}

	if len(fixtures) != 2 {
		t.Fatalf("expected 2 fixtures, got %d", len(fixtures))
	}

	// Find fixtures by name (order is filesystem-dependent)
	fixtureMap := make(map[string]*indexFixture)
	for _, f := range fixtures {
		fixtureMap[f.name] = f
	}

	t.Run("users index", func(t *testing.T) {
		users, ok := fixtureMap["users"]
		if !ok {
			t.Fatal("users fixture not found")
		}

		if users.mapping == nil {
			t.Error("expected mapping to be non-nil")
		}
		if users.settings == nil {
			t.Error("expected settings to be non-nil")
		}
		if len(users.documents) != 2 {
			t.Errorf("expected 2 documents, got %d", len(users.documents))
		}

		// Verify document IDs
		if users.documents[0].ID != "1" {
			t.Errorf("expected first document ID to be '1', got %q", users.documents[0].ID)
		}
		if users.documents[1].ID != "2" {
			t.Errorf("expected second document ID to be '2', got %q", users.documents[1].ID)
		}

		// Verify _id is removed from body
		if _, ok := users.documents[0].Body["_id"]; ok {
			t.Error("_id should be removed from document body")
		}

		// Verify document body fields
		if name, ok := users.documents[0].Body["name"].(string); !ok || name != "Alice" {
			t.Errorf("expected name 'Alice', got %v", users.documents[0].Body["name"])
		}
	})

	t.Run("products index", func(t *testing.T) {
		products, ok := fixtureMap["products"]
		if !ok {
			t.Fatal("products fixture not found")
		}

		if products.mapping == nil {
			t.Error("expected mapping to be non-nil")
		}
		// products has no _settings.json
		if products.settings != nil {
			t.Error("expected settings to be nil for products")
		}

		// 2 docs from 001_electronics.yml + 1 doc from 002_books.yml
		if len(products.documents) != 3 {
			t.Errorf("expected 3 documents, got %d", len(products.documents))
		}
	})
}

func TestParseFixtures_EmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	_, err := parseFixtures(dir)
	if err == nil {
		t.Fatal("expected error for empty directory")
	}
}

func TestParseFixtures_NonExistentDirectory(t *testing.T) {
	_, err := parseFixtures("/nonexistent/path")
	if err == nil {
		t.Fatal("expected error for non-existent directory")
	}
}

func TestParseFixtures_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	indexDir := filepath.Join(dir, "bad_index")
	if err := os.Mkdir(indexDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(indexDir, "_mapping.json"), []byte("{invalid}"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := parseFixtures(dir)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParseFixtures_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	indexDir := filepath.Join(dir, "bad_index")
	if err := os.Mkdir(indexDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(indexDir, "documents.yml"), []byte("not: [valid: yaml"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := parseFixtures(dir)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestParseFixtures_NoMappingOrSettings(t *testing.T) {
	dir := t.TempDir()
	indexDir := filepath.Join(dir, "dynamic_index")
	if err := os.Mkdir(indexDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(indexDir, "documents.yml"), []byte("- name: test\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	fixtures, err := parseFixtures(dir)
	if err != nil {
		t.Fatalf("parseFixtures() error: %v", err)
	}

	if len(fixtures) != 1 {
		t.Fatalf("expected 1 fixture, got %d", len(fixtures))
	}
	if fixtures[0].mapping != nil {
		t.Error("expected mapping to be nil")
	}
	if fixtures[0].settings != nil {
		t.Error("expected settings to be nil")
	}
	if len(fixtures[0].documents) != 1 {
		t.Errorf("expected 1 document, got %d", len(fixtures[0].documents))
	}
}

func TestParseFixtures_DocumentWithoutID(t *testing.T) {
	dir := t.TempDir()
	indexDir := filepath.Join(dir, "auto_id")
	if err := os.Mkdir(indexDir, 0o755); err != nil {
		t.Fatal(err)
	}
	yamlContent := "- name: test\n  value: 123\n"
	if err := os.WriteFile(filepath.Join(indexDir, "documents.yml"), []byte(yamlContent), 0o644); err != nil {
		t.Fatal(err)
	}

	fixtures, err := parseFixtures(dir)
	if err != nil {
		t.Fatalf("parseFixtures() error: %v", err)
	}

	doc := fixtures[0].documents[0]
	if doc.ID != "" {
		t.Errorf("expected empty ID, got %q", doc.ID)
	}
	if doc.Body["name"] != "test" {
		t.Errorf("expected name 'test', got %v", doc.Body["name"])
	}
}
