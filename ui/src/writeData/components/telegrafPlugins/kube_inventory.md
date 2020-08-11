# Kubernetes Inventory Input Plugin

This plugin generates metrics derived from the state of the following Kubernetes resources:

- daemonsets
- deployments
- endpoints
- ingress
- nodes
- persistentvolumes
- persistentvolumeclaims
- pods (containers)
- services
- statefulsets

Kubernetes is a fast moving project, with a new minor release every 3 months. As
such, we will aim to maintain support only for versions that are supported by
the major cloud providers; this is roughly 4 release / 2 years.

**This plugin supports Kubernetes 1.11 and later.**

#### Series Cardinality Warning

This plugin may produce a high number of series which, when not controlled
for, will cause high load on your database. Use the following techniques to
avoid cardinality issues:

- Use [metric filtering][] options to exclude unneeded measurements and tags.
- Write to a database with an appropriate [retention policy][].
- Limit series cardinality in your database using the
  [max-series-per-database][] and [max-values-per-tag][] settings.
- Consider using the [Time Series Index][tsi].
- Monitor your databases [series cardinality][].
- Consult the [InfluxDB documentation][influx-docs] for the most up-to-date techniques.

### Configuration:

```toml
[[inputs.kube_inventory]]
  ## URL for the Kubernetes API
  url = "https://127.0.0.1"

  ## Namespace to use. Set to "" to use all namespaces.
  # namespace = "default"

  ## Use bearer token for authorization. ('bearer_token' takes priority)
  ## If both of these are empty, we'll use the default serviceaccount:
  ## at: /run/secrets/kubernetes.io/serviceaccount/token
  # bearer_token = "/path/to/bearer/token"
  ## OR
  # bearer_token_string = "abc_123"

  ## Set response_timeout (default 5 seconds)
  # response_timeout = "5s"

  ## Optional Resources to exclude from gathering
  ## Leave them with blank with try to gather everything available.
  ## Values can be - "daemonsets", deployments", "endpoints", "ingress", "nodes",
  ## "persistentvolumes", "persistentvolumeclaims", "pods", "services", "statefulsets"
  # resource_exclude = [ "deployments", "nodes", "statefulsets" ]

  ## Optional Resources to include when gathering
  ## Overrides resource_exclude if both set.
  # resource_include = [ "deployments", "nodes", "statefulsets" ]

  ## selectors to include and exclude as tags.  Globs accepted.
  ## Note that an empty array for both will include all selectors as tags
  ## selector_exclude overrides selector_include if both set.
  selector_include = []
  selector_exclude = ["*"]

  ## Optional TLS Config
  # tls_ca = "/path/to/cafile"
  # tls_cert = "/path/to/certfile"
  # tls_key = "/path/to/keyfile"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false

  ## Uncomment to remove deprecated metrics.
  # fielddrop = ["terminated_reason"]
```

#### Kubernetes Permissions

If using [RBAC authorization](https://kubernetes.io/docs/reference/access-authn-authz/rbac/), you will need to create a cluster role to list "persistentvolumes" and "nodes". You will then need to make an [aggregated ClusterRole](https://kubernetes.io/docs/reference/access-authn-authz/rbac/#aggregated-clusterroles) that will eventually be bound to a user or group.

```yaml
---
kind: ClusterRole
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: influx:cluster:viewer
  labels:
    rbac.authorization.k8s.io/aggregate-view-telegraf: "true"
rules:
  - apiGroups: [""]
    resources: ["persistentvolumes", "nodes"]
    verbs: ["get", "list"]

---
kind: ClusterRole
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: influx:telegraf
aggregationRule:
  clusterRoleSelectors:
    - matchLabels:
        rbac.authorization.k8s.io/aggregate-view-telegraf: "true"
    - matchLabels:
        rbac.authorization.k8s.io/aggregate-to-view: "true"
rules: [] # Rules are automatically filled in by the controller manager.
```

Bind the newly created aggregated ClusterRole with the following config file, updating the subjects as needed.

```yaml
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: influx:telegraf:viewer
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: influx:telegraf
subjects:
  - kind: ServiceAccount
    name: telegraf
    namespace: default
```

### Metrics:

- kubernetes_daemonset
  - tags:
    - daemonset_name
    - namespace
    - selector (\*varies)
  - fields:
    - generation
    - current_number_scheduled
    - desired_number_scheduled
    - number_available
    - number_misscheduled
    - number_ready
    - number_unavailable
    - updated_number_scheduled

* kubernetes_deployment
  - tags:
    - deployment_name
    - namespace
    - selector (\*varies)
  - fields:
    - replicas_available
    - replicas_unavailable
    - created

- kubernetes_endpoints
  - tags:
    - endpoint_name
    - namespace
    - hostname
    - node_name
    - port_name
    - port_protocol
    - kind (\*varies)
  - fields:
    - created
    - generation
    - ready
    - port

* kubernetes_ingress
  - tags:
    - ingress_name
    - namespace
    - hostname
    - ip
    - backend_service_name
    - path
    - host
  - fields:
    - created
    - generation
    - backend_service_port
    - tls

- kubernetes_node
  - tags:
    - node_name
  - fields:
    - capacity_cpu_cores
    - capacity_memory_bytes
    - capacity_pods
    - allocatable_cpu_cores
    - allocatable_memory_bytes
    - allocatable_pods

* kubernetes_persistentvolume
  - tags:
    - pv_name
    - phase
    - storageclass
  - fields:
    - phase_type (int, [see below](#pv-phase_type))

- kubernetes_persistentvolumeclaim
  - tags:
    - pvc_name
    - namespace
    - phase
    - storageclass
    - selector (\*varies)
  - fields:
    - phase_type (int, [see below](#pvc-phase_type))

* kubernetes_pod_container
  - tags:
    - container_name
    - namespace
    - node_name
    - pod_name
    - node_selector (\*varies)
    - state
    - readiness
  - fields:
    - restarts_total
    - state_code
    - state_reason
    - terminated_reason (string, deprecated in 1.15: use `state_reason` instead)
    - resource_requests_cpu_units
    - resource_requests_memory_bytes
    - resource_limits_cpu_units
    - resource_limits_memory_bytes

- kubernetes_service
  - tags:
    - service_name
    - namespace
    - port_name
    - port_protocol
    - external_name
    - cluster_ip
    - selector (\*varies)
  - fields
    - created
    - generation
    - port
    - target_port

* kubernetes_statefulset
  - tags:
    - statefulset_name
    - namespace
    - selector (\*varies)
  - fields:
    - created
    - generation
    - replicas
    - replicas_current
    - replicas_ready
    - replicas_updated
    - spec_replicas
    - observed_generation

#### pv `phase_type`

The persistentvolume "phase" is saved in the `phase` tag with a correlated numeric field called `phase_type` corresponding with that tag value.

| Tag value | Corresponding field value |
| --------- | ------------------------- |
| bound     | 0                         |
| failed    | 1                         |
| pending   | 2                         |
| released  | 3                         |
| available | 4                         |
| unknown   | 5                         |

#### pvc `phase_type`

The persistentvolumeclaim "phase" is saved in the `phase` tag with a correlated numeric field called `phase_type` corresponding with that tag value.

| Tag value | Corresponding field value |
| --------- | ------------------------- |
| bound     | 0                         |
| lost      | 1                         |
| pending   | 2                         |
| unknown   | 3                         |

### Example Output:

```
kubernetes_configmap,configmap_name=envoy-config,namespace=default,resource_version=56593031 created=1544103867000000000i 1547597616000000000
kubernetes_daemonset,daemonset_name=telegraf,selector_select1=s1,namespace=logging number_unavailable=0i,desired_number_scheduled=11i,number_available=11i,number_misscheduled=8i,number_ready=11i,updated_number_scheduled=11i,created=1527758699000000000i,generation=16i,current_number_scheduled=11i 1547597616000000000
kubernetes_deployment,deployment_name=deployd,selector_select1=s1,namespace=default replicas_unavailable=0i,created=1544103082000000000i,replicas_available=1i 1547597616000000000
kubernetes_node,node_name=ip-172-17-0-2.internal allocatable_pods=110i,capacity_memory_bytes=128837533696,capacity_pods=110i,capacity_cpu_cores=16i,allocatable_cpu_cores=16i,allocatable_memory_bytes=128732676096 1547597616000000000
kubernetes_persistentvolume,phase=Released,pv_name=pvc-aaaaaaaa-bbbb-cccc-1111-222222222222,storageclass=ebs-1-retain phase_type=3i 1547597616000000000
kubernetes_persistentvolumeclaim,namespace=default,phase=Bound,pvc_name=data-etcd-0,selector_select1=s1,storageclass=ebs-1-retain phase_type=0i 1547597615000000000
kubernetes_pod,namespace=default,node_name=ip-172-17-0-2.internal,pod_name=tick1 last_transition_time=1547578322000000000i,ready="false" 1547597616000000000
kubernetes_service,cluster_ip=172.29.61.80,namespace=redis-cache-0001,port_name=redis,port_protocol=TCP,selector_app=myapp,selector_io.kompose.service=redis,selector_role=slave,service_name=redis-slave created=1588690034000000000i,generation=0i,port=6379i,target_port=0i 1547597616000000000
kubernetes_pod_container,container_name=telegraf,namespace=default,node_name=ip-172-17-0-2.internal,node_selector_node-role.kubernetes.io/compute=true,pod_name=tick1,state=running,readiness=ready resource_requests_cpu_units=0.1,resource_limits_memory_bytes=524288000,resource_limits_cpu_units=0.5,restarts_total=0i,state_code=0i,state_reason="",resource_requests_memory_bytes=524288000 1547597616000000000
kubernetes_statefulset,namespace=default,selector_select1=s1,statefulset_name=etcd replicas_updated=3i,spec_replicas=3i,observed_generation=1i,created=1544101669000000000i,generation=1i,replicas=3i,replicas_current=3i,replicas_ready=3i 1547597616000000000
```

[metric filtering]: https://github.com/influxdata/telegraf/blob/master/docs/CONFIGURATION.md#metric-filtering
[retention policy]: https://docs.influxdata.com/influxdb/latest/guides/downsampling_and_retention/
[max-series-per-database]: https://docs.influxdata.com/influxdb/latest/administration/config/#max-series-per-database-1000000
[max-values-per-tag]: https://docs.influxdata.com/influxdb/latest/administration/config/#max-values-per-tag-100000
[tsi]: https://docs.influxdata.com/influxdb/latest/concepts/time-series-index/
[series cardinality]: https://docs.influxdata.com/influxdb/latest/query_language/spec/#show-cardinality
[influx-docs]: https://docs.influxdata.com/influxdb/latest/
[k8s-telegraf]: https://www.influxdata.com/blog/monitoring-kubernetes-architecture/
