package testutils

import (
	"time"

	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/l7mp/dcontroller/pkg/object"
	"github.com/l7mp/dcontroller/pkg/reconciler"
)

// TryWatchReq attempts to receive a reconciler.Request from a channel within the specified timeout.
// Returns the request and true if successful, or an empty request and false if timeout occurs.
func TryWatchReq(watcher chan reconciler.Request, timeout time.Duration) (reconciler.Request, bool) {
	select {
	case req := <-watcher:
		return req, true
	case <-time.After(timeout):
		return reconciler.Request{}, false
	}
}

// TryWatch attempts to receive a watch.Event from a watch.Interface within the specified timeout.
// Returns the event and true if successful, or an empty event and false if timeout occurs.
func TryWatch(watcher watch.Interface, timeout time.Duration) (watch.Event, bool) {
	select {
	case event := <-watcher.ResultChan():
		return event, true
	case <-time.After(timeout):
		return watch.Event{}, false
	}
}

// MatchRequest validates that a reconciler.Request matches the expected values.
// It checks that the request contains an object snapshot and verifies namespace, name,
// event type, and GVK fields match the expected values.
//
// This helper should be used in tests to verify reconciler requests without re-fetching
// objects from the cache, which could introduce TOCTOU races.
func MatchRequest(req reconciler.Request, ns, name string, eventType object.DeltaType, gvk schema.GroupVersionKind) {
	Expect(req.Object).NotTo(BeNil(), "Request should contain object snapshot")
	Expect(req.Namespace).To(Equal(ns))
	Expect(req.Name).To(Equal(name))
	Expect(req.EventType).To(Equal(eventType))
	Expect(req.GVK).To(Equal(gvk))
}
