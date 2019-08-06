module github.com/influxdata/influxdb

go 1.12

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/NYTimes/gziphandler v1.0.1
	github.com/RoaringBitmap/roaring v0.4.16
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883
	github.com/apache/arrow/go/arrow v0.0.0-20190426170622-338c62a2a205
	github.com/aws/aws-sdk-go v1.16.15 // indirect
	github.com/benbjohnson/tmpl v1.0.0
	github.com/beorn7/perks v0.0.0-20180321164747-3a771d992973 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/bouk/httprouter v0.0.0-20160817010721-ee8b3818a7f5
	github.com/cespare/xxhash v1.1.0
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/coreos/bbolt v1.3.1-coreos.6
	github.com/davecgh/go-spew v1.1.1
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/dgryski/go-bitstream v0.0.0-20180413035011-3522498ce2c8
	github.com/docker/docker v1.13.1 // indirect
	github.com/editorconfig-checker/editorconfig-checker v0.0.0-20190219201458-ead62885d7c8
	github.com/elazarl/go-bindata-assetfs v1.0.0
	github.com/getkin/kin-openapi v0.2.0
	github.com/ghodss/yaml v1.0.0
	github.com/glycerine/go-unsnap-stream v0.0.0-20181221182339-f9677308dec2 // indirect
	github.com/glycerine/goconvey v0.0.0-20180728074245-46e3a41ad493 // indirect
	github.com/gogo/protobuf v1.2.1
	github.com/golang/gddo v0.0.0-20181116215533-9bd4a3295021
	github.com/golang/protobuf v1.3.1
	github.com/golang/snappy v0.0.1
	github.com/google/btree v0.0.0-20180813153112-4030bb1f1f0c
	github.com/google/go-cmp v0.3.0
	github.com/google/go-github v17.0.0+incompatible
	github.com/gopherjs/gopherjs v0.0.0-20181103185306-d547d1d9531e // indirect
	github.com/goreleaser/goreleaser v0.97.0
	github.com/hashicorp/go-msgpack v0.0.0-20150518234257-fa3f63826f7c // indirect
	github.com/hashicorp/raft v1.0.0 // indirect
	github.com/hashicorp/vault/api v1.0.2
	github.com/influxdata/flux v0.38.0
	github.com/influxdata/influxql v0.0.0-20180925231337-1cbfca8e56b6
	github.com/influxdata/usage-client v0.0.0-20160829180054-6d3895376368
	github.com/jessevdk/go-flags v1.4.0
	github.com/jsternberg/zap-logfmt v1.2.0
	github.com/jtolds/gls v4.2.1+incompatible // indirect
	github.com/julienschmidt/httprouter v1.2.0
	github.com/jwilder/encoding v0.0.0-20170811194829-b4e1701a28ef
	github.com/k0kubun/colorstring v0.0.0-20150214042306-9440f1994b88 // indirect
	github.com/kevinburke/go-bindata v3.11.0+incompatible
	github.com/mattn/go-isatty v0.0.4
	github.com/mattn/go-zglob v0.0.1 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1
	github.com/mna/pigeon v1.0.1-0.20180808201053-bb0192cfc2ae
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/nats-io/gnatsd v1.3.0 // indirect
	github.com/nats-io/go-nats v1.7.0 // indirect
	github.com/nats-io/go-nats-streaming v0.4.0
	github.com/nats-io/nats-streaming-server v0.11.2
	github.com/nats-io/nkeys v0.0.2 // indirect
	github.com/nats-io/nuid v1.0.0 // indirect
	github.com/onsi/ginkgo v1.7.0 // indirect
	github.com/onsi/gomega v1.4.3 // indirect
	github.com/opentracing/opentracing-go v1.1.0
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v0.9.0
	github.com/prometheus/client_model v0.0.0-20180712105110-5c3871d89910
	github.com/prometheus/common v0.0.0-20181020173914-7e9e6cabbd39
	github.com/prometheus/procfs v0.0.3 // indirect
	github.com/satori/go.uuid v1.2.0
	github.com/smartystreets/assertions v0.0.0-20180927180507-b2de0cb4f26d // indirect
	github.com/smartystreets/goconvey v0.0.0-20181108003508-044398e4856c // indirect
	github.com/spf13/cast v1.2.0
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.3
	github.com/spf13/viper v1.2.1
	github.com/tcnksm/go-input v0.0.0-20180404061846-548a7d7a8ee8
	github.com/testcontainers/testcontainers-go v0.0.0-20190108154635-47c0da630f72
	github.com/tinylib/msgp v1.1.0 // indirect
	github.com/tylerb/graceful v1.2.15
	github.com/uber-go/atomic v1.3.2 // indirect
	github.com/uber/jaeger-client-go v2.15.0+incompatible
	github.com/uber/jaeger-lib v1.5.0+incompatible // indirect
	github.com/willf/bitset v1.1.9 // indirect
	github.com/yudai/gojsondiff v1.0.0
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82 // indirect
	github.com/yudai/pp v2.0.1+incompatible // indirect
	go.uber.org/multierr v1.1.0
	go.uber.org/zap v1.9.1
	golang.org/x/crypto v0.0.0-20190308221718-c2843e01d9a2
	golang.org/x/net v0.0.0-20190404232315-eb5bcb51f2a3
	golang.org/x/oauth2 v0.0.0-20181017192945-9dcd33a902f4
	golang.org/x/sync v0.0.0-20190227155943-e225da77a7e6
	golang.org/x/sys v0.0.0-20190403152447-81d4e9dc473e
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	golang.org/x/tools v0.0.0-20190322203728-c1a832b0ad89
	google.golang.org/api v0.0.0-20181021000519-a2651947f503
	google.golang.org/grpc v1.19.1
	gopkg.in/editorconfig/editorconfig-core-go.v1 v1.3.0 // indirect
	gopkg.in/ini.v1 v1.42.0 // indirect
	gopkg.in/robfig/cron.v2 v2.0.0-20150107220207-be2e0b0deed5
	gopkg.in/vmihailenco/msgpack.v2 v2.9.1 // indirect
	honnef.co/go/tools v0.0.0-20190319011948-d116c56a00f3
	labix.org/v2/mgo v0.0.0-20140701140051-000000000287 // indirect
	launchpad.net/gocheck v0.0.0-20140225173054-000000000087 // indirect
)

replace github.com/Sirupsen/logrus => github.com/sirupsen/logrus v1.2.0

replace github.com/influxdata/platform => /dev/null

replace github.com/goreleaser/goreleaser => github.com/influxdata/goreleaser v0.97.0-influx
