package manager

import (
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	bmgr "sigs.k8s.io/controller-runtime/pkg/manager"

	"hsnlab/dcontroller-runtime/pkg/cache"
)

// logf "sigs.k8s.io/controller-runtime/pkg/log"

// Manager is a wrapper around the controller-runtime Manager. It implements similar calls as the
// wrapped manager, but the calls operate on the view cache, not the Kubernetes API server. In
// order to obtain the real manager, use manager.Manager.
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

// GetCache returns the view cache. Use manager.Manager.GetCache() to obtain the cache of the
// controller runtime manager.
func (m *Manager) GetCache() *cache.Cache {
	return m.cache
}
