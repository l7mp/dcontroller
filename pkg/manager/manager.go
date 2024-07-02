package manager

import (
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"hsnlab/dcontroller-runtime/pkg/cache"
)

// logf "sigs.k8s.io/controller-runtime/pkg/log"

type Manager struct {
	manager.Manager
	ctrls map[string]controller.Controller // a controller per view
	cache *cache.Cache                     // a cache per view
	views map[string]string                // view store
}

// // New creates a new dmanager
// func (m *Manager) New(config *rest.Config, options basemgr.Options) (*Manager, error) {
// 	mgr, err := basemgr.New(config, options)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &Manager{
// 		base:   mgr,
// 		ctrls:  make(map[string]controller.Controller),
// 		caches: make(map[string]cache.Cache),
// 		dep:    dag.New(),
// 	}, nil
// }

func (m *Manager) GetCache() *cache.Cache {
	return m.cache
}
