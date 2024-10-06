package controller

import (
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type testTrig struct{ counter int }

func (t *testTrig) Trigger(err error) { t.counter++ }

var _ = Describe("StatusReporter", func() {
	It("should be able to push errors and return the last", func() {
		r := NewErrorReporter(&testTrig{0})

		err := errors.New("1")
		Expect(r.PushError(err)).To(Equal(err))
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(1))
		Expect(r.IsCritical()).To(BeFalse())

		err = errors.New("2")
		Expect(r.PushError(err)).To(Equal(err))
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(2))
		Expect(r.IsCritical()).To(BeFalse())

		err = errors.New("3")
		Expect(r.PushError(err)).To(Equal(err))
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(3))
		Expect(r.IsCritical()).To(BeFalse())

		err = errors.New("4")
		Expect(r.PushError(err)).To(Equal(err))
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(4))
		Expect(r.trigger.(*testTrig).counter).To(Equal(3))
		Expect(r.IsCritical()).To(BeFalse())

		err = errors.New("5")
		Expect(r.PushCriticalError(err)).To(Equal(err))
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(5))
		Expect(r.trigger.(*testTrig).counter).To(Equal(3))
		Expect(r.IsCritical()).To(BeTrue())
	})

	It("should hold the last 10 errors", func() {
		r := NewErrorReporter(&testTrig{0})

		errs := [ErrorReporterStackSize + 10]error{}
		for i := 0; i < ErrorReporterStackSize+10; i++ {
			errs[i] = fmt.Errorf("%d", i)
			Expect(r.PushError(errs[i])).To(Equal(errs[i]))
		}
		Expect(r.Size()).To(Equal(ErrorReporterStackSize))
		Expect(r.Top()).To(Equal(errs[ErrorReporterStackSize+9]))

		Expect(r.trigger.(*testTrig).counter).To(Equal(3))
	})
})
