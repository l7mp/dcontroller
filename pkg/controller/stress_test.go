package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimeManager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/yaml"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/manager"
	"github.com/l7mp/dcontroller/pkg/object"
)

var _ = Describe("Controller stress tests", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(logr.NewContext(context.Background(), logger))
	})

	AfterEach(func() {
		cancel()
	})

	Context("Chain of controllers on the same view", func() {
		It("should process object through controller chain without stale events", func() {
			const (
				chainLength = 25
				viewName    = "chain-test-view"
			)

			mgr, err := manager.NewFakeManager(runtimeManager.Options{Logger: logger})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			go func() { mgr.Start(ctx) }()

			// Track how many times each controller is called
			callCounts := &sync.Map{}

			// Controller YAML template - we'll substitute the stage number
			controllerTemplate := `
name: ctrl-%d
sources:
  - apiGroup: test.view.dcontroller.io
    kind: view
    labelSelector:
      matchLabels:
        state: ctrl-%d
pipeline:
  - '@project':
      metadata:
        name: "$.metadata.name"
        namespace: "$.metadata.namespace"
        labels:
          state: ctrl-%d
      content: "$.content"
target:
  apiGroup: test.view.dcontroller.io
  kind: view
  type: Patcher`

			// Create the controller chain
			controllers := make([]Controller, chainLength)
			for i := 1; i <= chainLength; i++ {
				// Generate YAML for this controller
				// Controller i watches for "state: ctrl-i" and outputs "state: ctrl-(i+1)"
				nextState := i + 1
				if i == chainLength {
					nextState = chainLength // Last controller keeps the label as ctrl-N
				}

				yamlData := fmt.Sprintf(controllerTemplate, i, i, nextState)

				var config opv1a1.Controller
				err = yaml.Unmarshal([]byte(yamlData), &config)
				Expect(err).NotTo(HaveOccurred())

				// Create a wrapper that tracks invocations
				ctrlName := fmt.Sprintf("ctrl-%d", i)
				callCounts.Store(ctrlName, 0)

				c, err := NewDeclarative(mgr, "stress-test", config, Options{})
				Expect(err).NotTo(HaveOccurred())
				Expect(c.GetName()).To(Equal(ctrlName))

				controllers[i-1] = c

				log.Info("Created controller", "name", ctrlName,
					"watches", fmt.Sprintf("state: ctrl-%d", i),
					"produces", fmt.Sprintf("state: ctrl-%d", nextState))
			}

			// Give controllers time to start watching
			time.Sleep(200 * time.Millisecond)

			// Create the initial view object with label "state: ctrl-1"
			view := object.NewViewObject("test", "view")
			object.SetName(view, "default", viewName)
			object.SetContent(view, map[string]any{"data": "initial-value"})
			view.SetLabels(map[string]string{
				"state": "ctrl-1",
			})

			// Add the view to the view cache
			vcache := mgr.GetCompositeCache().GetViewCache()
			err = vcache.Add(view)
			Expect(err).NotTo(HaveOccurred())

			log.Info("Injected initial object", "labels", view.GetLabels())

			// Watch for the object to reach the final state (state: ctrl-N)
			finalLabel := fmt.Sprintf("ctrl-%d", chainLength)

			// Use Eventually to wait for the final state
			Eventually(func() bool {
				get := object.NewViewObject("test", "view")
				object.SetName(get, "default", viewName)

				err := vcache.Get(ctx, client.ObjectKeyFromObject(get), get)
				if err != nil {
					log.V(1).Info("Failed to get view", "error", err)
					return false
				}

				labels := get.GetLabels()
				if labels == nil {
					return false
				}

				state, ok := labels["state"]
				if !ok {
					return false
				}

				log.V(1).Info("Current state", "state", state, "target", finalLabel)
				return state == finalLabel
			}, 10*time.Second, 100*time.Millisecond).Should(BeTrue(),
				"Object should reach final state %s", finalLabel)

			log.Info("Object reached final state", "state", finalLabel)

			// Verify the object has the correct final label
			finalView := object.NewViewObject("test", "view")
			object.SetName(finalView, "default", viewName)
			err = vcache.Get(ctx, client.ObjectKeyFromObject(finalView), finalView)
			Expect(err).NotTo(HaveOccurred())

			labels := finalView.GetLabels()
			Expect(labels).NotTo(BeNil())
			Expect(labels["state"]).To(Equal(finalLabel))

			// TODO: Add mechanism to track and verify each controller is called exactly once
			// This will require instrumenting the reconciler or pipeline to count invocations
			// For now, we just verify the chain completes successfully

			log.Info("Stress test completed successfully")
		})
	})
})
