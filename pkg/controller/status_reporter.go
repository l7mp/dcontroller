package controller

import (
	"strings"
)

const ErrorReporterStackSize = 5

type ErrorReporter interface {
	Push(error) error
	Top() error
	Size() int
	IsEmpty() bool
}

type reporter struct {
	*errorStack
}

func NewErrorReporter() ErrorReporter {
	return &reporter{errorStack: &errorStack{errors: []error{}}}
}

func (r *reporter) Push(err error) error { r.errorStack.Push(err); return err }

// errorStack is a simple error stack implementation
type errorStack struct {
	errors []error
}

func (s *errorStack) Push(err error) {
	if len(s.errors) == ErrorReporterStackSize {
		copy(s.errors, s.errors[1:])
		s.errors[len(s.errors)-1] = err
		return
	}
	s.errors = append(s.errors, err)
}

func (s *errorStack) Pop() {
	if s.IsEmpty() {
		return
	}
	s.errors = s.errors[:len(s.errors)-1]
}

func (s *errorStack) Top() error {
	if s.IsEmpty() {
		return nil
	}
	return s.errors[len(s.errors)-1]
}

func (s *errorStack) Size() int {
	return len(s.errors)
}

func (s *errorStack) IsEmpty() bool {
	return len(s.errors) == 0
}

func (s *errorStack) String() string {
	errs := []string{}
	for _, err := range s.errors {
		errs = append(errs, err.Error())
	}
	return strings.Join(errs, ",")
}
