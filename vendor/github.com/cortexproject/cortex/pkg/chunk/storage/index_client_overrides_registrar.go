package storage

import "github.com/cortexproject/cortex/pkg/chunk"

type IndexClientFactoryFunc func() (chunk.IndexClient, error)

type IndexClientOverridesRegistrar map[string]IndexClientFactoryFunc

func NewIndexClientOverridesRegistrar() IndexClientOverridesRegistrar {
	return IndexClientOverridesRegistrar{}
}

func (ir IndexClientOverridesRegistrar) RegisterIndexClient(name string, factory IndexClientFactoryFunc) {
	ir[name] = factory
}

func (ir IndexClientOverridesRegistrar) NewIndexClient(name string, cfg Config, schemaCfg chunk.SchemaConfig) (chunk.IndexClient, error) {
	if factory, isOK := ir[name]; isOK {
		return factory()
	}

	return NewIndexClient(name, cfg, schemaCfg)
}
