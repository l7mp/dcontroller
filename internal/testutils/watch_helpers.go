package testutils

import (
	"time"

	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/l7mp/dcontroller/pkg/object"
)

// ReconcileRequest is an interface that abstracts reconciler.Request for testing.
// This allows testutils to provide helpers without importing the reconciler package.
type ReconcileRequest interface {
	GetNamespace() string
	GetName() string
	GetEventType() object.DeltaType
	GetGVK() schema.GroupVersionKind
	GetObject() object.Object
}

// TryWatchReq attempts to receive a reconciler.Request from a channel within the specified
// timeout.  Returns the value and true if successful, or a zero value and false if timeout occurs.
//
// Example usage:
//
//	req, ok := testutils.TryWatchReq(watcher, 100*time.Millisecond)
func TryWatchReq(watcher chan ReconcileRequest, timeout time.Duration) (ReconcileRequest, bool) {
	select {
	case req := <-watcher:
		return req, true
	case <-time.After(timeout):
		var zero ReconcileRequest
		return zero, false
	}
}

// TryWatchEvent attempts to receive a watch.Event from a watch.Interface within the specified timeout.
// Returns the event and true if successful, or an empty event and false if timeout occurs.
func TryWatchEvent(watcher watch.Interface, timeout time.Duration) (watch.Event, bool) {
	select {
	case event := <-watcher.ResultChan():
		return event, true
	case <-time.After(timeout):
		return watch.Event{}, false
	}
}

// MatchRequest validates that a ReconcileRequest matches the expected values.
// It checks that the request contains an object snapshot and verifies namespace, name,
// event type, and GVK fields match the expected values.
//
// This helper should be used in tests to verify reconciler requests without re-fetching
// objects from the cache, which could introduce TOCTOU races.
//
// Example usage:
//
//	req, ok := testutils.TryWatchChan(watcher, interval)
//	Expect(ok).To(BeTrue())
//	testutils.MatchRequest(req, "default", "viewname", object.Added, gvk)
func MatchRequest(req ReconcileRequest, ns, name string, eventType any, gvk schema.GroupVersionKind) {
	Expect(req.GetObject()).NotTo(BeNil(), "Request should contain object snapshot")
	Expect(req.GetNamespace()).To(Equal(ns))
	Expect(req.GetName()).To(Equal(name))
	Expect(req.GetEventType()).To(Equal(eventType))
	Expect(req.GetGVK()).To(Equal(gvk))
}
