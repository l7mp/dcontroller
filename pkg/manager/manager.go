package manager

import (
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	bmgr "sigs.k8s.io/controller-runtime/pkg/manager"

	"hsnlab/dcontroller-runtime/pkg/cache"
)

// logf "sigs.k8s.io/controller-runtime/pkg/log"

type Manager struct {
	bmgr.Manager
	ctrls map[string]controller.Controller // a controller per view
	cache *cache.Cache                     // a cache per view
	views map[string]string                // view store
	fake  bool
}

// New creates a new dmanager
func New(config *rest.Config, options bmgr.Options) (*Manager, error) {
	mgr, err := bmgr.New(config, options)
	if err != nil {
		return nil, err
	}

	return &Manager{
		Manager: mgr,
		ctrls:   make(map[string]controller.Controller),
		cache:   cache.New(),
	}, nil
}

func (m *Manager) GetCache() *cache.Cache {
	return m.cache
}

// comes from the manager
// func (m *Manager) GetScheme() *runtime.Scheme {
// 	return m.Manager.GetScheme()
// }
