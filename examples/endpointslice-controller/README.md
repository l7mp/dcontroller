# EndpointSlice operator

The EndpointSlice operator is a "hybrid" Kubernetes controller that demonstrates the use Δ-controller to manage the endpoint pool for a service. The operator is "hybrid" in that it is neither purely declarative nor purely imperative, in that it demonstrates how to use Δ-controller from imperative Go code, where instead of writing the objects emitted by a controller back into the Kubernetes API server, which is the default use case of Δ-controller and can be fully implemented in a declarative style, the output objects need to be processed in Go using an imperative style.

## Description

Certain use cases cannot be fully implemented in a purely declarative style, for instance because the Kubernetes operator needs to manipulate an imperative API. Such is the case if the task is to implement an **endpoint-discovery service** to collect the endpoints for a Kubernetes service and program an underlying system, say, a service mesh proxy, with the discovered endpoints. Δ-controller can come in handy in such cases as well, by letting the difficult part of the operator, the endpoint-discovery pipeline, to be implemented in a declarative form, leaving only the reconciliation logic, which updates the imperative API based on the endpoints discovered by the declarative controller, to be written in imperative Go.

This example demonstrates the use of Δ-controller in such a use case. The example code comprises two parts: an imperative **endpoint-discovery operator** that is written in Go using the Δ-controller API, and a declarative **controller pipeline** that automates the difficult part: generating the up-to-date list of endpoints for a Kubernetes Service based on the Kubernetes resources obtained from the API server.

### The controller pipeline

The declarative controller pipeline spec is read from a YAML manifest. There are two versions:
- `endpointslice-controller-spec.yaml`: this is the default spec, that will generate a separate view object per each (service, service-port, endpoint-address) combination. This is the one we discuss below.
- `endpointslice-controller-gather-spec.yaml`: the alternative spec gathers all separate endpoint addresses into view object that will hold a list of all the endpoints addresses for a (service, service-port) combination. This is mostly the same as the default spec, but it contains a final "gather" aggregation stage that will collapse the endpoint addresses into a list. See the YAML spec for the details.

The default declarative pipeline to controllers:
- the `service-controller` will watch the Kubernetes core.v1 Service API, generate a separate out object per each service-port, and load the resultant objects into a view called ServiceView.
- the `endpointslice-controller` watches the objects from the ServiceView and the EndpointSlice objects from the Kubernetes discovery.v1 API, pair service view objects with the corresponding EndpointSlices to expand it with the endpoint-addresses, filter addresses with `ready` status, demultiplex the endpoint addresses into a separate object and converts the shape of the resultant objects into a simpler form, and then load the results into a view called EndpointView. 

The idea is that the imperative controller will watch this EndpointView, which already contains the objects demultplexed and converted into the shape we want, instead of us having to write the imperative code to watch the Kubernetes API and perform the conversions ourselves. This helps cutting down development costs.

#### The Service controller

The task of this service is to generate a single object per each service-port in the watched Services. To simplify the task we will process only the Services annotated with `dcontroller.io/endpointslice-controller-enabled`. 

The pipeline is as follows.

1. Define the **name**: `name: service-controller`.

2. Set up the **source**: this is the API(s) that will be watched by the controller. In this case, this is the native Kubernetes core.v1 Service API (empty `apiGroup` means `core` and the version `v1` is automatically discovered by Δ-controller):

   ```yaml
    sources:
      - apiGroup: ""
        kind: Service
   ```

3. Create the **aggregation** pipeline. 

   This contains a single aggregation with 3 stages:
   - filter the Services annotated with `dcontroller.io/endpointslice-controller-enabled`. Note that we use the long-form JSONpath expression `$["metadata"][..."]` because the annotation contains a `/` that is incompatible with the simpler short-form,
   - remove some useless fields and converts the shape of the resultant objects,
   - demultiplex the result into multiple objects by blowing up the `$.spec.ports` list.

   The pipeline is as follows:

   ```yaml
   pipeline:
     "@aggregate":
       - "@select":
           "@exists": '$["metadata"]["annotations"]["dcontroller.io/endpointslice-controller-enabled"]'
       - "@project":
           metadata:
             name: $.metadata.name
             namespace: $.metadata.namespace
           spec:
             serviceName: $.metadata.name
             type: $.spec.type
             ports: $.spec.ports
       - "@unwind": $.spec.ports
   ```

4. Set up the **target** that will store the resultant deltas into the `ServiceView` view. Note that you don't have to define an `apiGroup`: en empty `apiGroup` means the target is a local view.

   ```yaml
   target:
     kind: ServiceView
   ```

#### The EndpointSlice controller

The task of this service is to pair ServiceView objects with the corresponding EndpointSlices and create a single output object per each endpoint address.

The pipeline is as follows.

1. Define the **name**: `name: endpointslice-controller`.

2. Set up the **source**: this controller watches two APIs, the Kubernetes native `discovery.v1` EndpointSlice API plus our own ServiceView API that contains the filtered and reprocessed Services.

  ```yaml
  sources:
    - kind: ServiceView
    - apiGroup: "discovery.k8s.io"
      kind: EndpointSlice
  ```

3. Define an **inner join** that will pair ServiceView objects with the corresponding EndpointSlice objects by checking whether the EndpointSlice is annotated with the Service name and the two objects are in the same namespace.  The resultant objects will contain two top-level maps: `$.ServiceView` will hold the matched ServiceView object and `$.EndpointSlice` will hold the EndpointSlice object.

   ```yaml
   "@join":
     "@and":
       - '@eq':
           - $.ServiceView.spec.serviceName
           - '$["EndpointSlice"]["metadata"]["labels"]["kubernetes.io/service-name"]'
       - '@eq':
           - $.ServiceView.metadata.namespace
           - $.EndpointSlice.metadata.namespace
   ```

4. Create the **aggregation** pipeline to convert the shape of the resultant, somewhat convoluted objects.

   The aggregation pipeline again has multiple stages:
   - set up the metadata, copy the ServiceView spec and the endpoints from the EndpointSlice, and create an `id` that will be used later to generate a unique stable name for the resultant objects,
   - demultiplex on the `$.endpoint` list,
   - filter ready addresses,
   - demultiplex again, now on the `$.endpoint.addresses` field of the original EndpointSlice object, and 
   - finally again convert the object shape: set a unique name by concatenating Service name with the hash of the object id (this name will be different per each (service, service-port, endpoint-address)) and copy the  relevant fields of the spec.

   ```yaml
   "@aggregate":
     - "@project":
         metadata:
           name: $.ServiceView.metadata.name
           namespace: $.ServiceView.metadata.namespace
         spec: $.ServiceView.spec
         endpoints: $.EndpointSlice.endpoints
         id:
           name: $.ServiceView.spec.serviceName
           namespace: $.ServiceView.metadata.namespace
           type: $.ServiceView.spec.type
           protocol: $.ServiceView.spec.ports.protocol
           port: $.ServiceView.spec.ports.port
           targetPort: $.ServiceView.spec.ports.targetPort
     - "@unwind": $.endpoints
     - "@select":
         "@eq": ["$.endpoints.conditions.ready", true]
     - "@unwind": $.endpoints.addresses
     - "@project":
         metadata:
           name:
             "@concat":
               - $.metadata.name
               - "-"
               - { "@hash": $.id }
           namespace: $.metadata.namespace
         spec:
           serviceName: $.spec.serviceName
           type: $.spec.type
           port: $.id.port
           targetPort: $.id.targetPort
           protocol: $.id.protocol
           address: $.endpoints.addresses
   ```

5. Set up the **target** to update the EndpointView view with the results.

   ```yaml
   target:
     kind: EndpointView
   ```



### The endoint-discovery operator

The operator will be written in Go. This will watch the EndpointView view as generated by the declarative pipeline for updates and implement the reconciliation logic. Again, the idea is that we don't have to write the tedious join+aggregation pipeline in imperative Go, instead we can implement this logic in a purely declarative style.

The Go code itself will differ too much from a standard [Kubernetes operator](https://book.kubebuilder.io/), just with the common packages taken from Δ-controller instead of the usual [Kubernetes controller runtime](https://pkg.go.dev/sigs.k8s.io/controller-runtime).

1. Define the usual Kubernetes operator boilerplate by importing packages, defining constants, parsing the command line arguments, and setting up a logger.
   
2. Create a Δ-controller manager:

   ```go
   mgr, err := dmanager.New(ctrl.GetConfigOrDie(), dmanager.Options{
       Options: ctrl.Options{Scheme: scheme},
   })
   if err != nil { ... }
   ```

3. Load the declarative operator pipeline from a file held in `specFile` (see later):

   ```go
   if _, err := doperator.NewFromFile("endpointslice-operator", mgr, specFile, opts); err != nil {
      ...
   }
   ```

4. Define the controller that will reconcile the events generated by the operator:

   ```go
   if _, err := NewEndpointSliceController(mgr, logger); err != nil { ... }
   ```

5. Start the manager (this will readily start our controller):

   ```go
   if err := mgr.Start(ctx); err != nil { ... }
   ```

The endpointslice controller below likewise follows the usual Kubernetes operator pattern: define an `endpointSliceController` struct that will represent our controller, write a constructor, and implement the `func Reconcile(context.Context, dreconciler.Request) (reconcile.Result, error)` function on the struct that will effectively process the events generated by the declarative controller.

The constructor will be called `NewEndpointSliceController`:

1. Construct the `endpointSliceController` struct:

   ```go
   r := &endpointSliceController{
      Client: mgr.GetClient(),
      log:    log.WithName("endpointSlice-ctrl"),
   }
   ```

2. Define the controller:

   ```go
   on := true
   c, err := controller.NewTyped("endpointslice-controller", mgr, controller.TypedOptions[dreconciler.Request]{
      SkipNameValidation: &on,
      Reconciler:         r,
   })
   if err != nil { ... }
   ```

3. Create a source for the `EndpointView` (this will be generated by the declarative part):

   ```go
   src, err := dreconciler.NewSource(mgr, opv1a1.Source{
      Resource: opv1a1.Resource{
         Kind: "EndpointView",
      },
   }).GetSource()
   if err != nil { ... }
   ```

4. And finally set up a watch that will bind our controller to the above source so that every time
   there is an update on the `EndpointView` our `Reconcile(...)` function will be called with
   the event to process it.

   ```go
   if err := c.Watch(src); err != nil { ... }
   ```

And finally the most important part, the `Reconcile(...)` function. Normally, this would be the
function that implements the business logic of our operator, say, by controlling a proxy with the
endpoints discovered by our operator. Here for simplicity we will just log the events and return a
successful reconcile result.

```go
func (r *endpointSliceController) Reconcile(ctx context.Context, req dreconciler.Request) (reconcile.Result, error) {
   r.log.Info("Reconciling", "request", req.String())

   switch req.EventType {
       case cache.Added, cache.Updated, cache.Upserted:
    
        r.log.Info("Add/update EndpointView object", "name", req.Name, "namespace", req.Namespace)
    
       // handle upsert event
    
       case cache.Deleted:
          r.log.Info("Delete EndpointView object", "name", req.Name, "namespace", req.Namespace)
    
       // handle delete event
    
       default:
          r.log.Info("Unhandled event", "name", req.Name, "namespace", req.Namespace, "type", req.EventType)
   }

   return reconcile.Result{}, nil
}
```

## Testing

Take off from an empty cluster and start the EndpointSlice controller:

```console
cd <project-root>
go run examples/endpointslice-controller/main.go -zap-log-level info -disable-endpoint-pooling
```

Deploy a sample deployment with to endpoints:

``` console
kubectl create deployment testdep --image=registry.k8s.io/pause:3.9 --replicas=2
```

Expose the deployment over two service-ports and annotate it so that the controller will catch it up:

``` console
kubectl apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: testsvc
  annotations:
    dcontroller.io/endpointslice-controller-enabled: "true"
spec:
  selector:
    app: testdep
  ports:
    - name: http
      protocol: TCP
      port: 80
    - name: https
      protocol: TCP
      port: 8843
EOF
```

The controller will emit 4 `Add` events for each object generated by the EndpointSlice controller, one per each (service, service-port, endpoint-address) combination:

``` console
INFO	endpointslice-ctrl	Add/update EndpointView object	{"name": "testsvc-b9p5tj", "namespace": "default", "spec": "map[string]interface {}{\"address\":\"10.244.1.69\", \"port\":80, \"protocol\":\"TCP\", \"serviceName\":\"testsvc\", \"targetPort\":80, \"type\":\"ClusterIP\"}"}
INFO	endpointslice-ctrl	Add/update EndpointView object	{"name": "testsvc-dg9n0j", "namespace": "default", "spec": "map[string]interface {}{\"address\":\"10.244.1.229\", \"port\":80, \"protocol\":\"TCP\", \"serviceName\":\"testsvc\", \"targetPort\":80, \"type\":\"ClusterIP\"}"}
INFO	endpointslice-ctrl	Add/update EndpointView object	{"name": "testsvc-6kq57l", "namespace": "default", "spec": "map[string]interface {}{\"address\":\"10.244.1.90\", \"port\":80, \"protocol\":\"TCP\", \"serviceName\":\"testsvc\", \"targetPort\":80, \"type\":\"ClusterIP\"}"}
INFO	endpointslice-ctrl	Add/update EndpointView object	{"name": "testsvc-43s657", "namespace": "default", "spec": "map[string]interface {}{\"address\":\"10.244.1.90\", \"port\":8843, \"protocol\":\"TCP\", \"serviceName\":\"testsvc\", \"targetPort\":8843, \"type\":\"ClusterIP\"}"}
```

Scale the deployment to 3 pods: this will generate another two further `Add` events, one for each new (service-port, endpoint-address) added:

``` console
kubectl scale deployment testdep --replicas=3
...
INFO	endpointslice-ctrl	Add/update EndpointView object	{"name": "testsvc-8x1zl2", "namespace": "default", "spec": "map[string]interface {}{\"address\":\"10.244.1.69\", \"port\":8843, \"protocol\":\"TCP\", \"serviceName\":\"testsvc\", \"targetPort\":8843, \"type\":\"ClusterIP\"}"}
INFO	endpointslice-ctrl	Add/update EndpointView object	{"name": "testsvc-8zbi3v", "namespace": "default", "spec": "map[string]interface {}{\"address\":\"10.244.1.229\", \"port\":8843, \"protocol\":\"TCP\", \"serviceName\":\"testsvc\", \"targetPort\":8843, \"type\":\"ClusterIP\"}"}
```

Deleting the Service will generate 6 `Delete` events for the objects generated above:

``` console
kubectl delete service testsvc
...
INFO	endpointslice-ctrl	Delete EndpointView object	{"name": "testsvc-6kq57l", "namespace": "default"}
INFO	endpointslice-ctrl	Delete EndpointView object	{"name": "testsvc-b9p5tj", "namespace": "default"}
INFO	endpointslice-ctrl	Delete EndpointView object	{"name": "testsvc-dg9n0j", "namespace": "default"}
INFO	endpointslice-ctrl	Delete EndpointView object	{"name": "testsvc-43s657", "namespace": "default"}
INFO	endpointslice-ctrl	Delete EndpointView object	{"name": "testsvc-8x1zl2", "namespace": "default"}
INFO	endpointslice-ctrl	Delete EndpointView object	{"name": "testsvc-8zbi3v", "namespace": "default"}
```

## Cleanup

Remove all resources we have created:

```console
kubectl delete deployments testdep
```

## License

Copyright 2025 by its authors. Some rights reserved. See [AUTHORS](AUTHORS).

Apache License - see [LICENSE](LICENSE) for full text.

