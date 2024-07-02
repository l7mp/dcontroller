package view

import (
	"context"
	"hsnlab/dcontroller-runtime/pkg/cache"

	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type View interface {
	GetName() string
	GetCache() *cache.Cache
	Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error)
	// Start(ctx context.Context) error
}
