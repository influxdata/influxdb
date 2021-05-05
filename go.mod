module github.com/influxdata/influxdb/v2

go 1.15

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/NYTimes/gziphandler v1.0.1
	github.com/RoaringBitmap/roaring v0.4.16
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883
	github.com/apache/arrow/go/arrow v0.0.0-20200923215132-ac86123a3f01
	github.com/benbjohnson/clock v0.0.0-20161215174838-7dc76406b6d3
	github.com/benbjohnson/tmpl v1.0.0
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/bouk/httprouter v0.0.0-20160817010721-ee8b3818a7f5
	github.com/buger/jsonparser v0.0.0-20191004114745-ee4c978eae7e
	github.com/cespare/xxhash v1.1.0
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/dgryski/go-bitstream v0.0.0-20180413035011-3522498ce2c8
	github.com/docker/docker v1.13.1 // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/editorconfig-checker/editorconfig-checker v0.0.0-20190819115812-1474bdeaf2a2
	github.com/elazarl/go-bindata-assetfs v1.0.0
	github.com/fatih/color v1.9.0
	github.com/fujiwara/shapeio v0.0.0-20170602072123-c073257dd745
	github.com/getkin/kin-openapi v0.2.0
	github.com/ghodss/yaml v1.0.0
	github.com/glycerine/go-unsnap-stream v0.0.0-20181221182339-f9677308dec2 // indirect
	github.com/glycerine/goconvey v0.0.0-20180728074245-46e3a41ad493 // indirect
	github.com/go-chi/chi v4.1.0+incompatible
	github.com/go-stack/stack v1.8.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/gddo v0.0.0-20181116215533-9bd4a3295021
	github.com/golang/mock v1.4.4
	github.com/golang/protobuf v1.3.3
	github.com/golang/snappy v0.0.1
	github.com/google/btree v1.0.0
	github.com/google/go-cmp v0.5.0
	github.com/google/go-github v17.0.0+incompatible
	github.com/google/go-jsonnet v0.14.0
	github.com/google/go-querystring v1.0.0 // indirect
	github.com/google/martian v2.1.1-0.20190517191504-25dcb96d9e51+incompatible // indirect
	github.com/hashicorp/go-msgpack v0.0.0-20150518234257-fa3f63826f7c // indirect
	github.com/hashicorp/go-retryablehttp v0.6.4 // indirect
	github.com/hashicorp/raft v1.0.0 // indirect
	github.com/hashicorp/vault/api v1.0.2
	github.com/imdario/mergo v0.3.9 // indirect
	github.com/influxdata/cron v0.0.0-20191203200038-ded12750aac6
	github.com/influxdata/flux v0.114.1
	github.com/influxdata/httprouter v1.3.1-0.20191122104820-ee83e2772f69
	github.com/influxdata/influxql v0.0.0-20180925231337-1cbfca8e56b6
	github.com/influxdata/pkg-config v0.2.7
	github.com/influxdata/usage-client v0.0.0-20160829180054-6d3895376368
	github.com/jessevdk/go-flags v1.4.0
	github.com/jsternberg/zap-logfmt v1.2.0
	github.com/jwilder/encoding v0.0.0-20170811194829-b4e1701a28ef
	github.com/k0kubun/colorstring v0.0.0-20150214042306-9440f1994b88 // indirect
	github.com/kevinburke/go-bindata v3.11.0+incompatible
	github.com/lib/pq v1.2.0 // indirect
	github.com/mattn/go-isatty v0.0.11
	github.com/matttproud/golang_protobuf_extensions v1.0.1
	github.com/mileusna/useragent v0.0.0-20190129205925-3e331f0949a5
	github.com/mna/pigeon v1.0.1-0.20180808201053-bb0192cfc2ae
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/nats-io/gnatsd v1.3.0
	github.com/nats-io/go-nats v1.7.0 // indirect
	github.com/nats-io/go-nats-streaming v0.4.0
	github.com/nats-io/nats-streaming-server v0.11.2
	github.com/nats-io/nkeys v0.0.2 // indirect
	github.com/nats-io/nuid v1.0.0 // indirect
	github.com/olekukonko/tablewriter v0.0.4
	github.com/onsi/ginkgo v1.11.0 // indirect
	github.com/onsi/gomega v1.8.1 // indirect
	github.com/opentracing/opentracing-go v1.1.0
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.9.1
	github.com/satori/go.uuid v1.2.1-0.20181028125025-b2ce2384e17b
	github.com/spf13/cast v1.3.0
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.6.1
	github.com/stretchr/testify v1.5.1
	github.com/tcnksm/go-input v0.0.0-20180404061846-548a7d7a8ee8
	github.com/testcontainers/testcontainers-go v0.0.0-20190108154635-47c0da630f72
	github.com/tinylib/msgp v1.1.0
	github.com/tylerb/graceful v1.2.15
	github.com/uber-go/atomic v1.3.2 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible
	github.com/uber/jaeger-lib v2.2.0+incompatible // indirect
	github.com/willf/bitset v1.1.9 // indirect
	github.com/xlab/treeprint v1.0.0
	github.com/yudai/gojsondiff v1.0.0
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82 // indirect
	github.com/yudai/pp v2.0.1+incompatible // indirect
	go.etcd.io/bbolt v1.3.5
	go.uber.org/multierr v1.5.0
	go.uber.org/zap v1.14.1
	golang.org/x/crypto v0.0.0-20200622213623-75b288015ac9
	golang.org/x/net v0.0.0-20200625001655-4c5254603344
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/sys v0.0.0-20200323222414-85ca7c5b95cd
	golang.org/x/text v0.3.2
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	golang.org/x/tools v0.0.0-20200721032237-77f530d86f9a
	google.golang.org/api v0.17.0
	gopkg.in/vmihailenco/msgpack.v2 v2.9.1 // indirect
	gopkg.in/yaml.v2 v2.3.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200121175148-a6ecf24a6d71
	honnef.co/go/tools v0.0.1-2020.1.4
	istio.io/pkg v0.0.0-20200606170016-70c5172b9cdf
	labix.org/v2/mgo v0.0.0-20140701140051-000000000287 // indirect
	launchpad.net/gocheck v0.0.0-20140225173054-000000000087 // indirect
)

// Arrow has been taking too long to merge our PR that addresses some checkptr fixes.
// We are using our own fork, which specifically applies the change in
// https://github.com/apache/arrow/pull/8112, on top of the commit of Arrow that flux uses.
//
// The next time Flux updates its Arrow dependency, we will see checkptr test failures,
// if that version does not include PR 8112. In that event, someone (perhaps Mark R again)
// will need to apply the change in 8112 on top of the newer version of Arrow.
replace github.com/apache/arrow/go/arrow v0.0.0-20191024131854-af6fa24be0db => github.com/influxdata/arrow/go/arrow v0.0.0-20200917142114-986e413c1705

replace github.com/nats-io/nats-streaming-server v0.11.2 => github.com/influxdata/nats-streaming-server v0.11.3-0.20201112040610-c277f7560803
