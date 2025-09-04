package controller

import (
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("StatusReporter", func() {
	It("should be able to push errors and return the last", func() {
		ch := make(chan error, 1)
		r := NewErrorReporter(ch)

		err := errors.New("1")
		Expect(r.PushError(err)).To(Equal(err))
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(1))
		Expect(r.HasCritical()).To(BeFalse())
		err2, ok := tryReadErrorChannel(ch, interval)
		Expect(ok).To(BeTrue())
		Expect(err2).To(Equal(err))

		err = errors.New("2")
		Expect(r.PushError(err)).To(Equal(err))
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(2))
		Expect(r.HasCritical()).To(BeFalse())
		err2, ok = tryReadErrorChannel(ch, interval)
		Expect(ok).To(BeTrue())
		Expect(err2).To(Equal(err))

		err = errors.New("3")
		Expect(r.PushError(err)).To(Equal(err))
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(3))
		Expect(r.HasCritical()).To(BeFalse())
		err2, ok = tryReadErrorChannel(ch, interval)
		Expect(ok).To(BeTrue())
		Expect(err2).To(Equal(err))

		err = errors.New("4")
		Expect(r.PushError(err)).To(Equal(err))
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(4))
		Expect(r.HasCritical()).To(BeFalse())
		_, ok = tryReadErrorChannel(ch, interval)
		// the rate limiter let's only the first 3 errors through
		Expect(ok).To(BeFalse())

		err = errors.New("5")
		Expect(r.PushCriticalError(err)).To(Equal(err))
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(5))
		Expect(r.HasCritical()).To(BeTrue())
		_, ok = tryReadErrorChannel(ch, interval)
		Expect(ok).To(BeFalse())
	})

	It("should hold the last 10 errors", func() {
		ch := make(chan error, ErrorReporterStackSize+11)
		r := NewErrorReporter(ch)

		errs := [ErrorReporterStackSize + 10]error{}
		for i := 0; i < ErrorReporterStackSize+10; i++ {
			errs[i] = fmt.Errorf("%d", i)
			Expect(r.PushError(errs[i])).To(Equal(errs[i]))
		}
		Expect(r.Size()).To(Equal(ErrorReporterStackSize))
		Expect(r.Top()).To(Equal(errs[ErrorReporterStackSize+9]))
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
