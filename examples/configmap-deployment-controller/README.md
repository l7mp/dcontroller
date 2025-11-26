# ConfigMap-Deployment operator: A NoCode Δ-controller operator

In this tutorial we are going to create a ConfigDeployment custom resource definition (CRD) and write a fully declarative controller that will implement the reconciler logic for this resource.

The goal is to manage a Deployment whose pods are always using the latest version of a ConfigMap. While ConfigMaps are auto-updated within Pods, applications may not always be able to auto-refresh config from the file system. Some applications require restarts to apply configuration updates, and this is the logic that our controller will implement.

## Description

The example will specify a NoCode Kubernetes operator taken from the [kubebuilder book](https://book.kubebuilder.io/reference/watching-resources/externally-managed.html?highlight=configmap#watch-linked-resources). The operator will implemented in a completely declarative form using the Δ-controller framework.

A ConfigDeployment CRD will hold a reference to a ConfigMap and a Deployment (both by name) inside its Spec, specifying the intent that the Deployment should be restarted whenever the corresponding ConfigMap is updated. All three objects must be in the same namespace.

The ConfigDeployment controller will be in charge of linking each ConfigMap with the corresponding Deployment, watching ConfigMaps and writing an annotation into the PodTemplate of the linked Deployment that will keep track of the latest version of the data within the referenced ConfigMap. Therefore when the version of the configMap is changed, the PodTemplate in the Deployment will change. This will cause a rolling upgrade of all Pods managed by the Deployment.

## Setup

Make sure Δ-controller is up and running. Then, load the ConfigDeployment custom resource definition:

```console
cd <project-root>
kubectl apply -f examples/configmap-deployment-controller/configdeployment-crd.yaml
```

## Operator

Insert the ConfigDeployment operator that will handle the CRD and implement the reconcile logic:

```console
kubectl apply -f examples/configmap-deployment-controller/configdeployment-operator.yaml
```

The operator definition includes a single controller, which consists of 4 parts:

The name specifies a unique controller name inside our operator that is used to refer to individual controllers. The `sources` field contains a set of Kubernetes API resources to watch. This time, the resources are the Deployments, the ConfigMaps, plus the ConfigDeployment CRD we have just created. The `target` field specifies a single API resource to update with the results. The type of the target is `Patches`, which indicates that the controller will use the processing pipeline output to patch the target. (In contrast, `Updater` targets will simply overwrite the target.)

The most important field is `pipeline`, which specifies a declarative pipeline to process the source API resources into the target resource. The fields of the source and the target resource can be specified using standard [JSONPath notation](https://datatracker.ietf.org/doc/html/rfc9535). 

Δ-controller comes with a rich aggregation and expression language that allows to manipulate structured objects in purely declarative way. The first `@join` op (if exists, this must always come first) describes how to combine the source resources. This is done by taking a Cartesian product of all source objects, creating a temporary object with one root-level field corresponding to each source resource type (so the ConfigMap will go into `$.ConfigMap`, Deployment into `$.Deployment`, etc.), and then evaluating the boolean expression on the result. The first `@eq` expression matches the `$.spec.configMap` field from the ConfigDeployment with the name of the ConfigMap, the second does likewise for Deployments, and the final two `@eq` ops makes sure that all three objects must be in the same namespace:

```yaml
"@join":
  "@and":
    - '@eq':
        - $.ConfigMap.metadata.name
        - $.ConfigDeployment.spec.configMap
    - '@eq':
        - $.Deployment.metadata.name
        - $.ConfigDeployment.spec.deployment
...
```

The second part of the pipeline specifies how to transform the objects selected by the join into a patch that will be used to update the target. The operations is fairly simple: we copy the Deployment name and namespace from the metadata (these will make sure we actually update the selected Deployment) and writes the ConfigMap's resource version into an annotation in the pod template:

```yaml
- "@project":
    metadata:
      name: "$.Deployment.metadata.name"
      namespace: "$.Deployment.metadata.namespace"
    spec:
      template:
        metadata:
          annotations:
            "dcontroller.io/configmap-version": "$.ConfigMap.metadata.resourceVersion"
...
```

And that's all. With about 40 lines of purely declarative and mostly self-explanatory YAML we have recreated the functionality of a textbook example operator that takes couple of hundreds of lines of Go plus a sizeable boilerplate.

## Test

We will test the operator with a ConfigMap and a Deployment, plus a ConfigDeployment resource that will link the two:

```console
kubectl create configmap config --from-literal=key1=value1 --from-literal=key2=value2
kubectl create deployment dep --image=docker.io/l7mp/net-debug --replicas=2
kubectl apply -f - <<EOF
apiVersion: dcontroller.io/v1alpha1
kind: ConfigDeployment
metadata:
  name: dep-config
spec:
  configMap: config
  deployment: dep
EOF
```

Check that the operator status indicates no errors:

```console
kubectl get operators.dcontroller.io configmap-deployment-operator -o yaml
apiVersion: dcontroller.io/v1alpha1
kind: Operator
metadata:
  name: configmap-deployment-operator
spec:
...
status:
  controllers:
  - name: configmap-deployment-controller
    conditions:
    - lastTransitionTime: "2024-10-03T12:26:02Z"
      message: Controller is up and running
      observedGeneration: 1
      reason: Ready
      status: "True"
      type: Ready
```

Let's check whether the resource version of the configmap actually appears as an annotation on the pods of the deployment:

```console
kubectl get configmaps config -o jsonpath='{.metadata.resourceVersion}'
12945
kubectl get pods -l app=dep -o jsonpath='{.items[0].metadata.annotations}'
{"dcontroller.io/configmap-version":"12945"}
```

So far so good. Now update the ConfigMap and watch how the pods get restarted:

```console
kubectl patch configmap/config --type merge -p '{"data":{"key3":"value3"}}'
sleep 5
kubectl get pods -l app=dep 
NAME                   READY   STATUS    RESTARTS   AGE
dep-846976656c-79gnf   1/1     Running   0          4s
dep-846976656c-cx5d7   1/1     Running   0          5s
```

The resource version should now be updated:

```console
kubectl get configmaps config -o jsonpath='{.metadata.resourceVersion}'
15886
kubectl get pods -l app=dep -o jsonpath='{.items[0].metadata.annotations}'
{"dcontroller.io/configmap-version":"15886"}
```

## Cleanup

Remove all resources we have created:

```console
kubectl delete configdeployments dep-config
kubectl delete configmaps my-config
kubectl delete deployments dep
kubectl delete operators.dcontroller.io configmap-deployment-operator
kubectl delete customresourcedefinitions configdeployments.dcontroller.io
```

## License

Copyright 2024 by its authors. Some rights reserved. See [AUTHORS](AUTHORS).

Apache License - see [LICENSE](LICENSE) for full text.

