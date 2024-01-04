module github.com/influxdata/influxdb

go 1.20

require (
	collectd.org v0.3.0
	github.com/BurntSushi/toml v0.3.1
	github.com/apache/arrow/go/arrow v0.0.0-20211112161151-bc219186db40
	github.com/benbjohnson/tmpl v1.0.0
	github.com/bmizerany/pat v0.0.0-20170815010413-6226ea591a40
	github.com/cespare/xxhash v1.1.0
	github.com/davecgh/go-spew v1.1.1
	github.com/dgryski/go-bitstream v0.0.0-20180413035011-3522498ce2c8
	github.com/go-chi/chi v4.1.0+incompatible
	github.com/golang-jwt/jwt v3.2.1+incompatible
	github.com/golang/mock v1.5.0
	github.com/golang/snappy v0.0.4
	github.com/google/go-cmp v0.5.8
	github.com/influxdata/flux v0.194.4
	github.com/influxdata/httprouter v1.3.1-0.20191122104820-ee83e2772f69
	github.com/influxdata/influxql v1.1.1-0.20211004132434-7e7d61973256
	github.com/influxdata/pkg-config v0.2.11
	github.com/influxdata/roaring v0.4.13-0.20180809181101-fc520f41fab6
	github.com/influxdata/usage-client v0.0.0-20160829180054-6d3895376368
	github.com/jsternberg/zap-logfmt v1.2.0
	github.com/jwilder/encoding v0.0.0-20170811194829-b4e1701a28ef
	github.com/klauspost/pgzip v1.0.2-0.20170402124221-0bf5dcad4ada
	github.com/mattn/go-isatty v0.0.14
	github.com/mileusna/useragent v0.0.0-20190129205925-3e331f0949a5
	github.com/opentracing/opentracing-go v1.2.0
	github.com/peterh/liner v1.0.1-0.20180619022028-8c1271fcf47f
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.26.0
	github.com/prometheus/prometheus v0.0.0-20200609090129-a6600f564e3c
	github.com/retailnext/hllpp v1.0.1-0.20180308014038-101a6d2f8b52
	github.com/spf13/cast v1.3.0
	github.com/spf13/cobra v0.0.3
	github.com/stretchr/testify v1.8.0
	github.com/tinylib/msgp v1.1.0
	github.com/uber/jaeger-client-go v2.28.0+incompatible
	github.com/xlab/treeprint v0.0.0-20180616005107-d6fb6747feb6
	go.uber.org/multierr v1.6.0
	go.uber.org/zap v1.16.0
	golang.org/x/crypto v0.16.0
	golang.org/x/exp v0.0.0-20231214170342-aacd6d4b4611
	golang.org/x/sync v0.5.0
	golang.org/x/sys v0.15.0
	golang.org/x/term v0.15.0
	golang.org/x/text v0.14.0
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	golang.org/x/tools v0.16.0
	google.golang.org/grpc v1.44.0
	google.golang.org/protobuf v1.28.1
)

require (
	cloud.google.com/go v0.82.0 // indirect
	cloud.google.com/go/bigquery v1.8.0 // indirect
	cloud.google.com/go/bigtable v1.10.1 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/azure-storage-blob-go v0.14.0 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.11.9 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.13 // indirect
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.3 // indirect
	github.com/Azure/go-autorest/autorest/azure/cli v0.4.2 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/DATA-DOG/go-sqlmock v1.4.1 // indirect
	github.com/Masterminds/semver v1.4.2 // indirect
	github.com/Masterminds/sprig v2.16.0+incompatible // indirect
	github.com/SAP/go-hdb v0.14.1 // indirect
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883 // indirect
	github.com/aokoli/goutils v1.0.1 // indirect
	github.com/apache/arrow/go/v7 v7.0.1 // indirect
	github.com/aws/aws-sdk-go v1.34.0 // indirect
	github.com/aws/aws-sdk-go-v2 v1.11.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.0.0 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.6.1 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.7.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.0.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.5.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.5.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.9.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.19.0 // indirect
	github.com/aws/smithy-go v1.9.0 // indirect
	github.com/benbjohnson/immutable v0.3.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bonitoo-io/go-sql-bigquery v0.3.4-1.4.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/deepmap/oapi-codegen v1.6.0 // indirect
	github.com/denisenkom/go-mssqldb v0.10.0 // indirect
	github.com/dimchansky/utfbom v1.1.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/eclipse/paho.mqtt.golang v1.2.0 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/form3tech-oss/jwt-go v3.2.5+incompatible // indirect
	github.com/gabriel-vasile/mimetype v1.4.0 // indirect
	github.com/glycerine/go-unsnap-stream v0.0.0-20180323001048-9f0cb55181dd // indirect
	github.com/go-sql-driver/mysql v1.5.0 // indirect
	github.com/goccy/go-json v0.9.6 // indirect
	github.com/gofrs/uuid v3.3.0+incompatible // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang/geo v0.0.0-20190916061304-5b978397cfec // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/flatbuffers v22.9.30-0.20221019131441-5792623df42e+incompatible // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/googleapis/gax-go/v2 v2.0.5 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/huandu/xstrings v1.0.0 // indirect
	github.com/imdario/mergo v0.3.5 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/influxdata/gosnowflake v1.6.9 // indirect
	github.com/influxdata/influxdb-client-go/v2 v2.3.1-0.20210518120617-5d1fff431040 // indirect
	github.com/influxdata/influxdb-iox-client-go v1.0.0-beta.1 // indirect
	github.com/influxdata/line-protocol v0.0.0-20200327222509-2487e7298839 // indirect
	github.com/influxdata/line-protocol/v2 v2.2.1 // indirect
	github.com/influxdata/tdigest v0.0.2-0.20210216194612-fc98d27c9e8b // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/klauspost/compress v1.14.2 // indirect
	github.com/klauspost/crc32 v0.0.0-20161016154125-cb6bfca970f6 // indirect
	github.com/lib/pq v1.0.0 // indirect
	github.com/mattn/go-colorable v0.1.9 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-runewidth v0.0.3 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.12 // indirect
	github.com/pkg/browser v0.0.0-20210911075715-681adbf594b8 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/segmentio/kafka-go v0.2.0 // indirect
	github.com/sergi/go-diff v1.1.0 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/uber-go/tally v3.3.15+incompatible // indirect
	github.com/uber/athenadriver v1.1.4 // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/vertica/vertica-sql-go v1.1.1 // indirect
	github.com/willf/bitset v1.1.9 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/mod v0.14.0 // indirect
	golang.org/x/net v0.19.0 // indirect
	golang.org/x/oauth2 v0.0.0-20210514164344-f6687ab2804c // indirect
	golang.org/x/xerrors v0.0.0-20220411194840-2f41105eb62f // indirect
	gonum.org/v1/gonum v0.11.0 // indirect
	google.golang.org/api v0.47.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20220126215142-9970aeb2e350 // indirect
	gopkg.in/yaml.v2 v2.3.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
