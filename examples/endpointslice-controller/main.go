package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/dcontroller/pkg/cache"
	"github.com/l7mp/dcontroller/pkg/manager"
	dobject "github.com/l7mp/dcontroller/pkg/object"
	doperator "github.com/l7mp/dcontroller/pkg/operator"
	dreconciler "github.com/l7mp/dcontroller/pkg/reconciler"
)

type NativeController controller.TypedController[dreconciler.Request]

const (
	OperatorName                    = "test-ep-operator"
	OperatorSpec                    = "examples/endpointslice-controller/endpointslice-controller-spec.yaml"
	OperatorGatherSpec              = "examples/endpointslice-controller/endpointslice-controller-gather-spec.yaml"
	EndpointSliceCtrlAnnotationName = "dcontroller.io/endpointslice-controller-enabled"
)

var (
	scheme                 = runtime.NewScheme()
	disableEndpointPooling *bool
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	disableEndpointPooling = flag.Bool("disable-endpoint-pooling", false,
		"Generate per-endpoint objects instead of a single object listing all service endpoints.")

	zapOpts := zap.Options{
		Development:     true,
		DestWriter:      os.Stderr,
		StacktraceLevel: zapcore.Level(3),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
	}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	logger := zap.New(zap.UseFlagOptions(&zapOpts))
	log := logger.WithName("epslice-op")
	ctrl.SetLogger(log)

	// Define the controller pipeline
	specFile := OperatorGatherSpec
	if *disableEndpointPooling {
		specFile = OperatorSpec
	}

	// Create an api
	config := ctrl.GetConfigOrDie()
	api, err := cache.NewAPI(config, cache.APIOptions{
		CacheOptions: cache.CacheOptions{Logger: logger},
	})
	if err != nil {
		log.Error(err, "unable to set up manager")
		os.Exit(1)
	}

	// Load the operator from file
	errorChan := make(chan error, 16)
	opts := doperator.Options{
		Cache:        api.GetCache(),
		ErrorChannel: errorChan,
		Logger:       logger,
	}

	// Load the operator from file. Do not call NewFromFile as that would commit the operator.
	op, err := doperator.NewFromFile(OperatorName, config, specFile, opts)
	if err != nil {
		log.Error(err, "unable to set up operator")
		os.Exit(1)
	}

	// Create the endpointslice controller
	r, err := NewEndpointSliceController(op.GetManager(), logger)
	if err != nil {
		log.Error(err, "failed to create endpointslice controller")
		os.Exit(1)
	}

	log.Info("created endpointslice controller")

	if err := op.AddNativeController("endpointslice-ctrl", r.GetController(), []schema.GroupVersionKind{}); err != nil {
		log.Error(err, "failed to add endpointslice controller to the operator")
		os.Exit(1)
	}

	if err := op.RegisterGVKs(); err != nil {
		log.Error(err, "failed to register GVKs")
		os.Exit(1)
	}

	// Create an error reporter thread
	ctx := ctrl.SetupSignalHandler()
	go func() {
		for {
			select {
			case <-ctx.Done():
				os.Exit(1)
			case err := <-errorChan:
				log.Error(err, "operator error")
			}
		}
	}()

	if err := op.Start(ctx); err != nil {
		log.Error(err, "problem running operator")
		os.Exit(1)
	}
}

// endpointSliceController implements the endpointSlice controller
type endpointSliceController struct {
	client.Client
	ctrl controller.TypedController[dreconciler.Request]
	log  logr.Logger
}

func NewEndpointSliceController(mgr manager.Manager, log logr.Logger) (*endpointSliceController, error) {
	r := &endpointSliceController{
		Client: mgr.GetClient(),
		log:    log.WithName("endpointslice-ctrl"),
	}

	on := true
	c, err := controller.NewTyped("endpointslice-controller", mgr, controller.TypedOptions[dreconciler.Request]{
		SkipNameValidation: &on,
		Reconciler:         r,
	})
	if err != nil {
		return nil, err
	}
	r.ctrl = c

	src, err := dreconciler.NewSource(mgr, OperatorName, opv1a1.Source{
		Resource: opv1a1.Resource{
			Kind: "EndpointView",
		},
	}).GetSource()
	if err != nil {
		return nil, fmt.Errorf("failed to create source: %w", err)
	}

	if err := c.Watch(src); err != nil {
		return nil, fmt.Errorf("failed to create watch: %w", err)
	}
	r.log.Info("created endpointslice controller")

	return r, nil
}

func (r *endpointSliceController) GetController() controller.TypedController[dreconciler.Request] {
	return r.ctrl
}

func (r *endpointSliceController) Reconcile(ctx context.Context, req dreconciler.Request) (reconcile.Result, error) {
	r.log.Info("Reconciling", "request", req.String())

	switch req.EventType {
	case dobject.Added, dobject.Updated, dobject.Upserted:
		obj := dobject.NewViewObject(OperatorName, req.GVK.Kind)
		if err := r.Get(ctx, types.NamespacedName{Name: req.Name, Namespace: req.Namespace}, obj); err != nil {
			r.log.Error(err, "failed to get added/updated object", "delta-type", req.EventType)
			return reconcile.Result{}, err
		}

		spec, ok, err := unstructured.NestedMap(obj.Object, "spec")
		if err != nil || !ok {
			return reconcile.Result{},
				fmt.Errorf("failed to look up added/updated object spec: %q", dobject.Dump(obj))
		}

		name := obj.GetName()
		namespace := obj.GetNamespace()

		r.log.Info("Add/update EndpointView object", "name", name, "namespace", namespace, "spec", fmt.Sprintf("%#v", spec))

		// handle upsert event

	case dobject.Deleted:
		r.log.Info("Delete EndpointView object", "name", req.Name, "namespace", req.Namespace)

		// handle delete event

	default:
		r.log.Info("Unhandled event", "name", req.Name, "namespace", req.Namespace, "type", req.EventType)
	}

	r.log.Info("Reconciliation done")

	return reconcile.Result{}, nil
}
