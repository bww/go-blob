package blob

type ReadConfig struct{}

func (c ReadConfig) WithOptions(opts []ReadOption) ReadConfig {
	for _, opt := range opts {
		c = opt(c)
	}
	return c
}

type ReadOption func(ReadConfig) ReadConfig

type WriteConfig struct {
	ContentType string
}

func (c WriteConfig) WithOptions(opts []WriteOption) WriteConfig {
	for _, opt := range opts {
		c = opt(c)
	}
	return c
}

type WriteOption func(WriteConfig) WriteConfig

func WithContentType(t string) WriteOption {
	return func(c WriteConfig) WriteConfig {
		c.ContentType = t
		return c
	}
}
