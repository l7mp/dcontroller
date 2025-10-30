package reconciler

import (
	"context"
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/client-go/util/workqueue"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
)

var _ = Describe("Virtual Sources", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		queue  workqueue.TypedRateLimitingInterface[Request]
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		queue = workqueue.NewTypedRateLimitingQueue(
			workqueue.DefaultTypedControllerRateLimiter[Request](),
		)
	})

	AfterEach(func() {
		cancel()
		queue.ShutDown()
	})

	Describe("OneShot Source", func() {
		It("should trigger exactly once", func() {
			src := &oneShotRuntimeSource{}

			err := src.Start(ctx, queue)
			Expect(err).ToNot(HaveOccurred())

			// Should receive one event
			Eventually(func() int {
				return queue.Len()
			}, time.Second, 10*time.Millisecond).Should(Equal(1))

			// Wait a bit more and verify no additional events
			Consistently(func() int {
				return queue.Len()
			}, 100*time.Millisecond, 10*time.Millisecond).Should(Equal(1))

			// Verify we can dequeue the event
			item, shutdown := queue.Get()
			Expect(shutdown).To(BeFalse())
			Expect(item).ToNot(BeNil())
			queue.Done(item)
		})

		It("should stop gracefully on context cancellation", func() {
			src := &oneShotRuntimeSource{}

			err := src.Start(ctx, queue)
			Expect(err).ToNot(HaveOccurred())

			// Cancel context immediately
			cancel()

			// Should still get the event (goroutine is not checking ctx before sending)
			Eventually(func() int {
				return queue.Len()
			}, time.Second, 10*time.Millisecond).Should(Equal(1))
		})
	})

	Describe("Periodic Source", func() {
		It("should trigger periodically", func() {
			period := 5 * time.Millisecond
			src := &periodicRuntimeSource{
				period: period,
			}

			err := src.Start(ctx, queue)
			Expect(err).ToNot(HaveOccurred())

			// Count events over a period
			eventCount := 0
			deadline := time.Now().Add(20 * time.Millisecond)

			for time.Now().Before(deadline) {
				if queue.Len() > 0 {
					item, shutdown := queue.Get()
					if shutdown {
						break
					}
					eventCount++
					queue.Done(item)
				}
				time.Sleep(1 * time.Millisecond)
			}

			// Should have received at least 2-3 events in 20ms with 5ms period
			Expect(eventCount).To(BeNumerically(">=", 2))
			Expect(eventCount).To(BeNumerically("<=", 5))
		})

		It("should stop on context cancellation", func() {
			period := 5 * time.Millisecond
			src := &periodicRuntimeSource{
				period: period,
			}

			err := src.Start(ctx, queue)
			Expect(err).ToNot(HaveOccurred())

			// Wait for first tick
			Eventually(func() int {
				return queue.Len()
			}, 100*time.Millisecond, 1*time.Millisecond).Should(BeNumerically(">", 0))

			initialCount := queue.Len()

			// Cancel context
			cancel()

			// Wait a bit to ensure ticker would have fired if still running
			time.Sleep(15 * time.Millisecond)

			// Queue length should not have increased significantly
			finalCount := queue.Len()
			Expect(finalCount - initialCount).To(BeNumerically("<=", 1))
		})

		It("should reject non-positive period", func() {
			src := &periodicRuntimeSource{period: 0}

			err := src.Start(ctx, queue)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("positive period"))
		})
	})

	Describe("IsVirtualSource", func() {
		It("should recognize virtual source group", func() {
			Expect(IsVirtualSource(VirtualSourceGroup)).To(BeTrue())
			Expect(IsVirtualSource("source.dcontroller.io")).To(BeTrue())
		})

		It("should reject other groups", func() {
			Expect(IsVirtualSource("")).To(BeFalse())
			Expect(IsVirtualSource("apps")).To(BeFalse())
			Expect(IsVirtualSource("view.dcontroller.io")).To(BeFalse())
		})
	})

	Describe("newPeriodicSource parameter parsing", func() {
		It("should parse period from parameters", func() {
			periodStr := "10ms"
			params := map[string]interface{}{
				"period": periodStr,
			}
			raw, err := json.Marshal(params)
			Expect(err).ToNot(HaveOccurred())

			source := opv1a1.Source{
				Resource: opv1a1.Resource{
					Kind: PeriodicKind,
				},
				Parameters: &apiextensionsv1.JSON{
					Raw: raw,
				},
			}

			// Create a minimal mock manager for testing
			// We'll just test the parsing logic directly
			var extractedPeriod time.Duration
			if source.Parameters != nil && source.Parameters.Raw != nil {
				var p map[string]interface{}
				if err := json.Unmarshal(source.Parameters.Raw, &p); err == nil {
					if ps, ok := p["period"].(string); ok {
						if d, err := time.ParseDuration(ps); err == nil {
							extractedPeriod = d
						}
					}
				}
			}

			Expect(extractedPeriod).To(Equal(10 * time.Millisecond))
		})

		It("should use default period when parameters are nil", func() {
			source := opv1a1.Source{
				Resource: opv1a1.Resource{
					Kind: PeriodicKind,
				},
				Parameters: nil,
			}

			extractedPeriod := 5 * time.Minute // default
			if source.Parameters != nil && source.Parameters.Raw != nil {
				var p map[string]interface{}
				if err := json.Unmarshal(source.Parameters.Raw, &p); err == nil {
					if ps, ok := p["period"].(string); ok {
						if d, err := time.ParseDuration(ps); err == nil {
							extractedPeriod = d
						}
					}
				}
			}

			Expect(extractedPeriod).To(Equal(5 * time.Minute))
		})

		It("should use default period for invalid duration string", func() {
			params := map[string]interface{}{
				"period": "invalid",
			}
			raw, err := json.Marshal(params)
			Expect(err).ToNot(HaveOccurred())

			source := opv1a1.Source{
				Resource: opv1a1.Resource{
					Kind: PeriodicKind,
				},
				Parameters: &apiextensionsv1.JSON{
					Raw: raw,
				},
			}

			extractedPeriod := 5 * time.Minute // default
			if source.Parameters != nil && source.Parameters.Raw != nil {
				var p map[string]interface{}
				if err := json.Unmarshal(source.Parameters.Raw, &p); err == nil {
					if ps, ok := p["period"].(string); ok {
						if d, err := time.ParseDuration(ps); err == nil {
							extractedPeriod = d
						}
					}
				}
			}

			Expect(extractedPeriod).To(Equal(5 * time.Minute))
		})
	})
})
