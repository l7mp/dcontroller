package controller

import (
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("StatusReporter", func() {
	It("should be able to push errors and return the last", func() {
		c := &Controller{name: "test-ctrl", op: "test-op"}
		ch := make(chan error, 1)
		r := NewErrorReporter(c, ch)

		err := r.PushError("1")
		var operr Error
		ok := errors.As(err, &operr)
		Expect(ok).To(BeTrue())
		Expect(operr.Operator).To(Equal("test-op"))
		Expect(operr.Controller).To(Equal("test-ctrl"))
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(1))
		Expect(r.HasCritical()).To(BeFalse())
		err2, ok := tryReadErrorChannel(ch, interval)
		Expect(ok).To(BeTrue())
		Expect(err2).To(Equal(err))

		err = r.Push(errors.New("2"))
		ok = errors.As(err, &operr)
		Expect(ok).To(BeTrue())
		Expect(operr.Operator).To(Equal("test-op"))
		Expect(operr.Controller).To(Equal("test-ctrl"))
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(2))
		Expect(r.HasCritical()).To(BeFalse())
		err2, ok = tryReadErrorChannel(ch, interval)
		Expect(ok).To(BeTrue())
		Expect(err2).To(Equal(err))

		err = r.PushErrorf("3")
		ok = errors.As(err, &operr)
		Expect(ok).To(BeTrue())
		Expect(operr.Operator).To(Equal("test-op"))
		Expect(operr.Controller).To(Equal("test-ctrl"))
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(3))
		Expect(r.HasCritical()).To(BeFalse())
		err2, ok = tryReadErrorChannel(ch, interval)
		Expect(ok).To(BeTrue())
		Expect(err2).To(Equal(err))
		ok = errors.As(err, &operr)
		Expect(ok).To(BeTrue())
		Expect(operr.Operator).To(Equal("test-op"))
		Expect(operr.Controller).To(Equal("test-ctrl"))

		err = r.PushError("4")
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(4))
		Expect(r.HasCritical()).To(BeFalse())
		_, ok = tryReadErrorChannel(ch, interval)
		// the rate limiter let's only the first 3 errors through
		Expect(ok).To(BeFalse())
		ok = errors.As(err, &operr)
		Expect(ok).To(BeTrue())
		Expect(operr.Operator).To(Equal("test-op"))
		Expect(operr.Controller).To(Equal("test-ctrl"))

		err = r.PushCriticalError("5")
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(5))
		Expect(r.HasCritical()).To(BeTrue())
		_, ok = tryReadErrorChannel(ch, interval)
		Expect(ok).To(BeFalse())
	})

	It("should hold the last 10 errors", func() {
		ch := make(chan error, ErrorReporterStackSize+11)
		c := &Controller{name: "test-ctrl", op: "test-op"}
		r := NewErrorReporter(c, ch)

		errs := [ErrorReporterStackSize + 10]error{}
		for i := 0; i < ErrorReporterStackSize+10; i++ {
			errs[i] = r.PushErrorf("%d", i)
		}
		Expect(r.Size()).To(Equal(ErrorReporterStackSize))

		err := r.Top()
		Expect(err).To(Equal(errs[ErrorReporterStackSize+9]))
		var operr Error
		ok := errors.As(err, &operr)
		Expect(ok).To(BeTrue())
		Expect(operr.Operator).To(Equal("test-op"))
		Expect(operr.Controller).To(Equal("test-ctrl"))
	})
})

func tryReadErrorChannel(ch chan error, d time.Duration) (error, bool) {
	select {
	case err := <-ch:
		return err, true
	case <-time.After(d):
		return nil, false
	}
}
