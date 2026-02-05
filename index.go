package testfixtures

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

// createIndex creates an Elasticsearch index with the given mapping and settings.
func createIndex(ctx context.Context, client *elasticsearch.Client, name string, mapping, settings json.RawMessage) error {
	body, err := buildCreateIndexBody(mapping, settings)
	if err != nil {
		return fmt.Errorf("building request body: %w", err)
	}

	var opts []func(*esapi.IndicesCreateRequest)
	if body != nil {
		opts = append(opts, client.Indices.Create.WithBody(bytes.NewReader(body)))
	}
	opts = append(opts, client.Indices.Create.WithContext(ctx))

	res, err := client.Indices.Create(name, opts...)
	if err != nil {
		return fmt.Errorf("creating index %q: %w", name, err)
	}
	defer res.Body.Close()

	if err := checkResponse(res); err != nil {
		return fmt.Errorf("creating index %q: %w", name, err)
	}

	return nil
}

// buildCreateIndexBody constructs the JSON body for the Create Index API.
func buildCreateIndexBody(mapping, settings json.RawMessage) ([]byte, error) {
	if mapping == nil && settings == nil {
		return nil, nil
	}

	body := make(map[string]json.RawMessage)
	if mapping != nil {
		body["mappings"] = mapping
	}
	if settings != nil {
		body["settings"] = settings
	}

	return json.Marshal(body)
}

// deleteIndex deletes an Elasticsearch index.
func deleteIndex(ctx context.Context, client *elasticsearch.Client, name string) error {
	res, err := client.Indices.Delete(
		[]string{name},
		client.Indices.Delete.WithContext(ctx),
		client.Indices.Delete.WithIgnoreUnavailable(true),
	)
	if err != nil {
		return fmt.Errorf("deleting index %q: %w", name, err)
	}
	defer res.Body.Close()

	if err := checkResponse(res); err != nil {
		return fmt.Errorf("deleting index %q: %w", name, err)
	}

	return nil
}

// bulkInsertDocuments inserts documents into an Elasticsearch index using BulkIndexer.
func bulkInsertDocuments(ctx context.Context, client *elasticsearch.Client, indexName string, docs []document) error {
	if len(docs) == 0 {
		return nil
	}

	indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client: client,
		Index:  indexName,
	})
	if err != nil {
		return fmt.Errorf("creating bulk indexer for %q: %w", indexName, err)
	}

	var bulkErrors []string
	for _, doc := range docs {
		body, err := json.Marshal(doc.Body)
		if err != nil {
			return fmt.Errorf("marshaling document: %w", err)
		}

		item := esutil.BulkIndexerItem{
			Action: "index",
			Body:   bytes.NewReader(body),
			OnFailure: func(_ context.Context, _ esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				if err != nil {
					bulkErrors = append(bulkErrors, err.Error())
				} else {
					bulkErrors = append(bulkErrors, fmt.Sprintf("[%d] %s: %s", res.Status, res.Error.Type, res.Error.Reason))
				}
			},
		}

		if doc.ID != "" {
			item.DocumentID = doc.ID
		}

		if err := indexer.Add(ctx, item); err != nil {
			return fmt.Errorf("adding document to bulk indexer: %w", err)
		}
	}

	if err := indexer.Close(ctx); err != nil {
		return fmt.Errorf("closing bulk indexer for %q: %w", indexName, err)
	}

	if len(bulkErrors) > 0 {
		return fmt.Errorf("bulk insert errors for %q: %s", indexName, strings.Join(bulkErrors, "; "))
	}

	stats := indexer.Stats()
	if stats.NumFailed > 0 {
		return fmt.Errorf("bulk insert for %q: %d documents failed", indexName, stats.NumFailed)
	}

	return nil
}

// refreshIndex forces a refresh on the index so documents are immediately searchable.
func refreshIndex(ctx context.Context, client *elasticsearch.Client, name string) error {
	res, err := client.Indices.Refresh(
		client.Indices.Refresh.WithIndex(name),
		client.Indices.Refresh.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("refreshing index %q: %w", name, err)
	}
	defer res.Body.Close()

	if err := checkResponse(res); err != nil {
		return fmt.Errorf("refreshing index %q: %w", name, err)
	}

	return nil
}

// checkResponse checks an Elasticsearch API response for errors.
func checkResponse(res *esapi.Response) error {
	if !res.IsError() {
		return nil
	}

	body, _ := io.ReadAll(res.Body)
	return fmt.Errorf("elasticsearch error [%s]: %s", res.Status(), string(body))
}
