package testsuite

import (
	"context"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/go-logr/logr"
	"go.uber.org/zap/zapcore"
	appsv1 "k8s.io/api/apps/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/l7mp/dcontroller/internal/testutils"
	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
)

type Suite struct {
	Timeout, Interval time.Duration
	LogLevel          int
	Cfg               *rest.Config
	Scheme            *runtime.Scheme
	K8sClient         client.Client
	TestEnv           *envtest.Environment
	Ctx               context.Context
	Cancel            context.CancelFunc
	Log               logr.Logger
}

func New(loglevel int, crds ...string) (*Suite, error) {
	s := &Suite{
		Timeout:  time.Second * 5,
		Interval: time.Millisecond * 250,
		LogLevel: loglevel,
		Scheme:   runtime.NewScheme(),
	}

	opts := zap.Options{
		Development:     true,
		DestWriter:      GinkgoWriter,
		StacktraceLevel: zapcore.Level(4),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
		Level:           zapcore.Level(loglevel), //nolint:gosec
	}
	log := zap.New(zap.UseFlagOptions(&opts))
	s.Log = log
	ctrl.SetLogger(log)

	s.Ctx, s.Cancel = context.WithCancel(context.Background())

	if err := clientgoscheme.AddToScheme(s.Scheme); err != nil {
		return nil, err
	}
	if err := appsv1.AddToScheme(s.Scheme); err != nil {
		return nil, err
	}
	if err := discoveryv1.AddToScheme(s.Scheme); err != nil {
		return nil, err
	}
	if err := opv1a1.AddToScheme(s.Scheme); err != nil {
		return nil, err
	}

	By("bootstrapping test environment")
	s.TestEnv = &envtest.Environment{
		CRDDirectoryPaths: append(
			[]string{filepath.Join("..", "..", "config", "crd", "resources")},
			crds...,
		),
		ErrorIfCRDPathMissing:    true,
		AttachControlPlaneOutput: true,
	}

	// cfg is defined in this file globally.
	var err error
	s.Cfg, err = s.TestEnv.Start()
	if err != nil {
		return nil, err
	}

	// a spearate client whose client.Reader does not go through the caches
	s.K8sClient, err = client.New(s.Cfg, client.Options{Scheme: s.Scheme})
	if err != nil {
		return nil, err
	}

	// create default test namespace
	if err := s.K8sClient.Create(s.Ctx, testutils.TestNs); err != nil {
		return nil, err
	}

	// create another testing namespace
	if err := s.K8sClient.Create(s.Ctx, testutils.TestNs2); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Suite) Close() {
	if err := s.K8sClient.Delete(s.Ctx, testutils.TestNs); err != nil {
		s.Log.Error(err, "removing test namespace")
	}
	if err := s.K8sClient.Delete(s.Ctx, testutils.TestNs2); err != nil {
		s.Log.Error(err, "removing test namespace")
	}

	s.Cancel()

	if err := s.TestEnv.Stop(); err != nil {
		s.Log.Error(err, "tearing down the test environment")
	}
}

func TimestampEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format(time.RFC3339Nano))
}
