# Test Bench

The test bench makes it easier to stage deployment scenarios to:

- experiment and test with novel multi-node architectures
- simulate workarounds beyond what is possible on developer machines
- test complex interplay between components

## Architecture
The test bench is meant to run in [Kubernetes]. To create different test setups or to customize a setup to a specific
cluster we use [Kustomize]. The [Kustomize] configs can be found in `k8s/`.

To glue IOx to [Kubernetes] we need some helper code which can be found under `k8s/base/glue-entrypoint.bash`.

## Getting Started
We'll "kustomize" the test bench. You can use `k8s/overlays/demo` as a starting point. You may wanna
[adjust images](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/images/) or change the router and
database configs. Then you can deploy the whole thing:

```console
$ kubectl kustomize ./k8s/overlays/demo| kubectl apply -f -
```

Now IOx should be ready for experiments. You might just feed it with some data:

```console
$ # in another terminal:
$ kubectl port-forward service/iox-router-service 8080:8080 8082:8082

$ # in your main terminal:
$ cd ../iox_data_generator/
$ cargo run --release -- \
    --spec schemas/cap-write.toml \
    --continue \
    --host 127.0.0.1:8080 \
    --token arbitrary \
    --org myorg \
    --bucket mybucket
```


[Kustomize]: https://kustomize.io/
[Kubernetes]: https://kubernetes.io/
