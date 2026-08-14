package main

import (
	"github.com/example/mesh/internal/config"
	"github.com/example/mesh/internal/provider"
	filesrc "github.com/example/mesh/internal/provider/file"
	httpsrc "github.com/example/mesh/internal/provider/http"
	k8ssrc "github.com/example/mesh/internal/provider/k8s"
)

// registerProviders installs the three discovery sources. This lives in
// main rather than in internal/provider, because a parent package that
// defines an interface must not import the packages that implement it.
func registerProviders(m *provider.Manager) {
	m.Register(filesrc.Name,
		func(cfg *config.Config) (provider.Provider, error) {
			return filesrc.New(cfg.Providers.File), nil
		},
		func(cfg *config.Config) bool { return cfg.Providers.File.Enabled },
		func(cfg *config.Config) any { return cfg.Providers.File },
	)

	m.Register(httpsrc.Name,
		func(cfg *config.Config) (provider.Provider, error) {
			return httpsrc.New(cfg.Providers.HTTP)
		},
		func(cfg *config.Config) bool { return cfg.Providers.HTTP.Enabled },
		func(cfg *config.Config) any { return cfg.Providers.HTTP },
	)

	m.Register(k8ssrc.Name,
		func(cfg *config.Config) (provider.Provider, error) {
			return k8ssrc.New(cfg.Providers.K8s)
		},
		func(cfg *config.Config) bool { return cfg.Providers.K8s.Enabled },
		func(cfg *config.Config) any { return cfg.Providers.K8s },
	)
}

