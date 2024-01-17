package blob

type ReadConfig struct {
}

func (c ReadConfig) WithOptions(opts ...ReadOption) ReadConfig {
	for _, opt := range opts {
		c = opt(c)
	}
	return c
}

type ReadOption func(ReadConfig) ReadConfig

type WriteConfig struct {
}

func (c WriteConfig) WithOptions(opts ...WriteOption) WriteConfig {
	for _, opt := range opts {
		c = opt(c)
	}
	return c
}

type WriteOption func(WriteConfig) WriteConfig
