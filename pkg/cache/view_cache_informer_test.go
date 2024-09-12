package cache

import (
	"k8s.io/client-go/tools/cache"
)

var _ cache.SharedIndexInformer = &ViewCacheInformer{}
