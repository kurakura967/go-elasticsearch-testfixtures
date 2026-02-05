package sample_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	testfixtures "github.com/kurakura967/go-elasticsearch-testfixtures"
)

var (
	client   *elasticsearch.Client
	fixtures *testfixtures.Loader
)

func TestMain(m *testing.M) {
	addr := os.Getenv("ELASTICSEARCH_URL")
	if addr == "" {
		addr = "http://localhost:9200"
	}

	var err error
	client, err = elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{addr},
	})
	if err != nil {
		log.Fatalf("creating ES client: %v", err)
	}

	fixtures, err = testfixtures.New(
		client,
		testfixtures.Directory("testdata/fixtures"),
	)
	if err != nil {
		log.Fatalf("creating fixtures loader: %v", err)
	}

	os.Exit(m.Run())
}

func TestSearchUsers(t *testing.T) {
	if err := fixtures.Load(); err != nil {
		t.Fatalf("loading fixtures: %v", err)
	}
	t.Cleanup(func() { fixtures.Clean() })

	// Search for users with age >= 30
	query := `{
		"query": {
			"range": {
				"age": { "gte": 30 }
			}
		}
	}`

	res, err := client.Search(
		client.Search.WithContext(context.Background()),
		client.Search.WithIndex("users"),
		client.Search.WithBody(strings.NewReader(query)),
	)
	if err != nil {
		t.Fatalf("search error: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		t.Fatalf("search response error: %s", res.Status())
	}

	var result searchResult
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		t.Fatalf("decoding response: %v", err)
	}

	// Alice (30) and Charlie (35) should match
	if result.Hits.Total.Value != 2 {
		t.Errorf("expected 2 hits, got %d", result.Hits.Total.Value)
	}

	names := make(map[string]bool)
	for _, hit := range result.Hits.Hits {
		names[fmt.Sprintf("%v", hit.Source["name"])] = true
	}
	if !names["Alice"] {
		t.Error("expected Alice in results")
	}
	if !names["Charlie"] {
		t.Error("expected Charlie in results")
	}
}

func TestGetProductByID(t *testing.T) {
	if err := fixtures.Load(); err != nil {
		t.Fatalf("loading fixtures: %v", err)
	}
	t.Cleanup(func() { fixtures.Clean() })

	// Retrieve a specific product by its document ID
	res, err := client.Get("products", "p1",
		client.Get.WithContext(context.Background()),
	)
	if err != nil {
		t.Fatalf("get error: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		t.Fatalf("get response error: %s", res.Status())
	}

	var doc getResult
	if err := json.NewDecoder(res.Body).Decode(&doc); err != nil {
		t.Fatalf("decoding response: %v", err)
	}

	if title := fmt.Sprintf("%v", doc.Source["title"]); title != "Laptop" {
		t.Errorf("expected title 'Laptop', got %q", title)
	}
}

func TestFilterProductsByCategory(t *testing.T) {
	if err := fixtures.Load(); err != nil {
		t.Fatalf("loading fixtures: %v", err)
	}
	t.Cleanup(func() { fixtures.Clean() })

	// Filter products by category
	query := `{
		"query": {
			"term": {
				"category": "electronics"
			}
		}
	}`

	res, err := client.Search(
		client.Search.WithContext(context.Background()),
		client.Search.WithIndex("products"),
		client.Search.WithBody(strings.NewReader(query)),
	)
	if err != nil {
		t.Fatalf("search error: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		t.Fatalf("search response error: %s", res.Status())
	}

	var result searchResult
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		t.Fatalf("decoding response: %v", err)
	}

	// Laptop and Smartphone are in "electronics"
	if result.Hits.Total.Value != 2 {
		t.Errorf("expected 2 electronics products, got %d", result.Hits.Total.Value)
	}
}

func TestLoadResetsState(t *testing.T) {
	if err := fixtures.Load(); err != nil {
		t.Fatalf("loading fixtures: %v", err)
	}
	t.Cleanup(func() { fixtures.Clean() })

	// Insert an extra document manually
	res, err := client.Index(
		"users",
		strings.NewReader(`{"name": "Extra User", "email": "extra@example.com", "age": 99}`),
		client.Index.WithContext(context.Background()),
		client.Index.WithRefresh("true"),
	)
	if err != nil {
		t.Fatalf("indexing extra document: %v", err)
	}
	res.Body.Close()

	// Reload fixtures — state should be reset
	if err := fixtures.Load(); err != nil {
		t.Fatalf("reloading fixtures: %v", err)
	}

	// Count should be back to 3 (original fixture data only)
	countRes, err := client.Count(
		client.Count.WithContext(context.Background()),
		client.Count.WithIndex("users"),
	)
	if err != nil {
		t.Fatalf("counting documents: %v", err)
	}
	defer countRes.Body.Close()

	var countResult struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(countRes.Body).Decode(&countResult); err != nil {
		t.Fatalf("decoding count: %v", err)
	}

	if countResult.Count != 3 {
		t.Errorf("expected 3 users after reload, got %d", countResult.Count)
	}
}

// Helper types for decoding ES responses

type searchResult struct {
	Hits struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
		Hits []struct {
			ID     string                 `json:"_id"`
			Source map[string]interface{} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type getResult struct {
	ID     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
}
