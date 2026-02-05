package testfixtures

import "context"

// Option configures the Loader.
type Option func(*Loader) error

// Directory sets the path to the fixtures directory.
// This option is required.
func Directory(dir string) Option {
	return func(l *Loader) error {
		l.dir = dir
		return nil
	}
}

// WithContext sets the default context for Elasticsearch operations.
// If not set, context.Background() is used.
func WithContext(ctx context.Context) Option {
	return func(l *Loader) error {
		l.ctx = ctx
		return nil
	}
}
