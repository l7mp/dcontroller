package manager

//logf "sigs.k8s.io/controller-runtime/pkg/log"

// type Manager struct {
// 	base   basemgr.Manager
// 	ctrls  map[string]controller.Controller // a controller per view
// 	caches map[string]cache.Cache           // a cache per view
// 	dep    *dag.Graph                       // the dependency graph
// }

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

// // Register starts to watch the base K8s API resource defined in obj and creates a view eith the
// // given name, possibly applying the aggregation on the base view.
// func (m *Manager) Register(name string, obj client.Object, aggr aggregate.Aggregate) (*view.View, error) {
// 	r := &view.View{
// 		mgr:  *Manager,
// 		name: name,
// 		aggr: aggr,
// 	}

// 	c, err := basectrl.New(name, m.base, basectrl.Options{Reconciler: r})
// 	if err != nil {
// 		return nil, err
// 	}

// 	if err := c.Watch(source.Kind(m.base.GetCache(), obj,
// 		&handler.EnqueueRequestForObject{},
// 		predicate.GenerationChangedPredicate{},
// 	)); err != nil {
// 		return nil, err
// 	}

// 	return r, nil

// }
