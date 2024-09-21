package controller

import (
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("StatusReporter", func() {
	It("should be able to push errors and return the last", func() {
		r := NewErrorReporter()

		err := errors.New("1")
		Expect(r.Push(err)).To(Equal(err))
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(1))

		err = errors.New("2")
		Expect(r.Push(err)).To(Equal(err))
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(2))

		err = errors.New("3")
		Expect(r.Push(err)).To(Equal(err))
		Expect(r.Top()).To(Equal(err))
		Expect(r.Size()).To(Equal(3))
	})

	It("should hold the last 5 errors", func() {
		r := NewErrorReporter()

		errs := [8]error{}
		for i := 0; i < 8; i++ {
			errs[i] = errors.New(fmt.Sprintf("%d", i))
			Expect(r.Push(errs[i])).To(Equal(errs[i]))
		}
		Expect(r.Top()).To(Equal(errs[7]))
		Expect(r.Size()).To(Equal(5))
	})
})
