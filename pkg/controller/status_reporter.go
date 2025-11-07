package controller

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

const (
	// ErrorReporterStackSize controls the depth of the LIFO error buffer.
	ErrorReporterStackSize int = 10

	// TrimPrefixSuffixLen contols the number of characters to retain at the prefix and the
	// suffix of long strings.
	TrimPrefixSuffixLen = 120
)

// RateLimit controls the status updater rate-limiter so that the first 3 errors will trigger an
// update per every 2 seconds.
func getDefaultRateLimiter() rate.Sometimes {
	return rate.Sometimes{First: 3, Interval: 2 * time.Second}
}

// Error is a error wrapped with an operator and a controller name.
type Error struct {
	error
	Operator, Controller string
}

// errorReporter is the error stack implementation.
type errorReporter struct {
	c           *DeclarativeController
	errorStack  []error
	ratelimiter rate.Sometimes
	errorChan   chan error
	critical    bool // whether a critical error has been reported
}

// NewErrorReporter creates a new error reporter.
func NewErrorReporter(c *DeclarativeController, errorChan chan error) *errorReporter {
	return &errorReporter{c: c, errorStack: []error{}, ratelimiter: getDefaultRateLimiter(), errorChan: errorChan}
}

// Push pushes a noncritical error to the error stack.
func (s *errorReporter) Push(err error) error {
	return s.push(err)
}

// PushError pushes a simple noncritical error to the error stack.
func (s *errorReporter) PushError(msg string) error {
	return s.push(errors.New(msg))
}

// PushErrorf pushes a formatted noncritical error to the error stack.
func (s *errorReporter) PushErrorf(format string, a ...any) error {
	return s.push(fmt.Errorf(format, a...))
}

// PushCritical pushes a critical error to the error stack.
func (s *errorReporter) PushCritical(err error) error {
	s.critical = true
	return s.push(err)
}

// PushCriticalError pushes a critical error to the error stack.
func (s *errorReporter) PushCriticalError(msg string) error {
	s.critical = true
	return s.push(errors.New(msg))
}

// PushCriticalErrorf pushes a formatted critical error to the error stack.
func (s *errorReporter) PushCriticalErrorf(format string, a ...any) error {
	s.critical = true
	return s.push(fmt.Errorf(format, a...))
}

// Push pushes a critical or non-critical error to the stack.
func (s *errorReporter) push(err error) error {
	ctrlErr := Error{Operator: s.c.op, Controller: s.c.name, error: err}

	// ask a status update if trigger is set
	defer s.ratelimiter.Do(func() {
		if s.errorChan != nil {
			s.errorChan <- ctrlErr
		}
	})

	if len(s.errorStack) == ErrorReporterStackSize {
		copy(s.errorStack, s.errorStack[1:])
		s.errorStack[len(s.errorStack)-1] = ctrlErr
		return ctrlErr
	}
	s.errorStack = append(s.errorStack, ctrlErr)

	return ctrlErr
}

// Pop pops the extra error from the stack.
func (s *errorReporter) Pop() {
	if s.IsEmpty() {
		return
	}
	s.errorStack = s.errorStack[:len(s.errorStack)-1]
}

// Top returns the last errors from the stack.
func (s *errorReporter) Top() error {
	if s.IsEmpty() {
		return nil
	}
	return s.errorStack[len(s.errorStack)-1]
}

// Size returns the number of errors on the stack.
func (s *errorReporter) Size() int {
	return len(s.errorStack)
}

// IsEmpty returns true if the stack is empty.
func (s *errorReporter) IsEmpty() bool {
	return len(s.errorStack) == 0
}

// HasCritical returns true if the stack contains a critical error.
func (s *errorReporter) HasCritical() bool {
	return s.critical
}

// Report returns the error messages on the stack.
func (s *errorReporter) Report() []string {
	errs := []string{}
	for _, err := range s.errorStack {
		errs = append(errs, trim(err.Error()))
	}
	return errs
}

// String stringifies the error stack.
func (s *errorReporter) String() string {
	return strings.Join(s.Report(), ",")
}

// trim shortens a string list.
func trim(s string) string {
	if len(s) <= 2*TrimPrefixSuffixLen+5 {
		return s
	}

	return s[0:TrimPrefixSuffixLen-1] + "[...]" + s[len(s)-TrimPrefixSuffixLen:]
}
