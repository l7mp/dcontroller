/*
Copyright 2022 The l7mp/stunner team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"go.uber.org/zap/zapcore"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	opv1a1 "hsnlab/dcontroller/pkg/api/operator/v1alpha1"
)

var _ = fmt.Sprintf("%d", 1)

// Define utility constants for object names and testing timeouts/durations and intervals.
const (
	timeout  = time.Second * 10
	interval = time.Millisecond * 250
	// loglevel = -10
	loglevel = -3
	//loglevel = -1
)

var (
	// Resources
	testNs = &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "testnamespace",
		},
	}

	// Globals
	cfg              *rest.Config
	scheme           *runtime.Scheme = runtime.NewScheme()
	k8sClient        client.Client
	testEnv          *envtest.Environment
	ctx              context.Context
	cancel, opCancel context.CancelFunc
	logger, setupLog logr.Logger
)

var _ = BeforeSuite(func() {
	opts := zap.Options{
		Development:     true,
		DestWriter:      GinkgoWriter,
		StacktraceLevel: zapcore.Level(3),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
		Level:           zapcore.Level(loglevel),
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	logger = ctrl.Log
	setupLog = logger.WithName("setup")

	ctx, cancel = context.WithCancel(context.Background())

	err := clientgoscheme.AddToScheme(scheme)
	Expect(err).NotTo(HaveOccurred())
	err = appsv1.AddToScheme(scheme)
	Expect(err).NotTo(HaveOccurred())
	err = discoveryv1.AddToScheme(scheme)
	Expect(err).NotTo(HaveOccurred())
	err = opv1a1.AddToScheme(scheme)
	Expect(err).NotTo(HaveOccurred())

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "config", "crd", "resources"),
		},
		ErrorIfCRDPathMissing:    true,
		AttachControlPlaneOutput: true,
	}

	// cfg is defined in this file globally.
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	// a spearate client whose client.Reader does not go through the caches
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	setupLog.Info("creating a testing namespace")
	Expect(k8sClient.Create(ctx, testNs)).Should(Succeed())

	setupLog.Info("creating another testing namespace")
	otherNs := testNs.DeepCopy()
	otherNs.SetName("other")
	otherNs.SetResourceVersion("")
	Expect(k8sClient.Create(ctx, otherNs)).Should(Succeed())
})

var _ = AfterSuite(func() {
	By("removing test namespace")
	Expect(k8sClient.Delete(ctx, testNs)).Should(Succeed())

	cancel()

	By("tearing down the test environment")
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})

func TimestampEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format(time.RFC3339Nano))
}

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)

	// for gingko/v2
	// suiteConfig, reporterConfig := GinkgoConfiguration()
	// reporterConfig.FullTrace = true
	// RunSpecs(t, "Controller Suite", suiteConfig, reporterConfig)

	RunSpecs(t, "Integration test")
}
