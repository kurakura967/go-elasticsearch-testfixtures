package testfixtures

import (
	"context"
	"errors"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8"
)

// Loader manages Elasticsearch test fixtures.
// It creates indices with mappings/settings and inserts test documents
// from fixture files organized in a directory structure.
type Loader struct {
	client   *elasticsearch.Client
	dir      string
	ctx      context.Context
	fixtures []*indexFixture
}

// New creates a new Loader with the given Elasticsearch client and options.
// The Directory option is required.
//
// Fixture files are parsed during construction, so any file format errors
// are reported immediately.
func New(client *elasticsearch.Client, opts ...Option) (*Loader, error) {
	if client == nil {
		return nil, errors.New("testfixtures: client must not be nil")
	}

	l := &Loader{
		client: client,
		ctx:    context.Background(),
	}

	for _, opt := range opts {
		if err := opt(l); err != nil {
			return nil, fmt.Errorf("testfixtures: applying option: %w", err)
		}
	}

	if l.dir == "" {
		return nil, errors.New("testfixtures: Directory option is required")
	}

	fixtures, err := parseFixtures(l.dir)
	if err != nil {
		return nil, fmt.Errorf("testfixtures: %w", err)
	}
	l.fixtures = fixtures

	return l, nil
}

// Load deletes existing managed indices, recreates them with their
// schema definitions, inserts fixture documents, and refreshes the indices
// so that documents are immediately searchable.
func (l *Loader) Load() error {
	for _, f := range l.fixtures {
		indexName := f.name

		if err := deleteIndex(l.ctx, l.client, indexName); err != nil {
			return fmt.Errorf("testfixtures: %w", err)
		}

		if err := createIndex(l.ctx, l.client, indexName, f.mapping, f.settings); err != nil {
			return fmt.Errorf("testfixtures: %w", err)
		}

		if err := bulkInsertDocuments(l.ctx, l.client, indexName, f.documents); err != nil {
			return fmt.Errorf("testfixtures: %w", err)
		}

		if err := refreshIndex(l.ctx, l.client, indexName); err != nil {
			return fmt.Errorf("testfixtures: %w", err)
		}
	}

	return nil
}

// Clean deletes all indices managed by this Loader.
func (l *Loader) Clean() error {
	var errs []error
	for _, f := range l.fixtures {
		if err := deleteIndex(l.ctx, l.client, f.name); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("testfixtures: cleaning up: %w", errors.Join(errs...))
	}

	return nil
}
