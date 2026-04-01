module github.com/influxdata/influxdb/v2

go 1.25.8

require (
	github.com/BurntSushi/toml v1.4.0
	github.com/Masterminds/squirrel v1.5.0
	github.com/NYTimes/gziphandler v1.0.1
	github.com/RoaringBitmap/roaring v0.4.16
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883
	github.com/apache/arrow-go/v18 v18.4.0
	github.com/benbjohnson/clock v1.1.0
	github.com/benbjohnson/tmpl v1.0.0
	github.com/buger/jsonparser v1.1.1
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc
	github.com/dgryski/go-bitstream v0.0.0-20180413035011-3522498ce2c8
	github.com/dustin/go-humanize v1.0.1
	github.com/editorconfig-checker/editorconfig-checker v0.0.0-20190819115812-1474bdeaf2a2
	github.com/elazarl/go-bindata-assetfs v1.0.1
	github.com/go-chi/chi v4.1.0+incompatible
	github.com/go-crypt/crypt v0.3.2
	github.com/go-stack/stack v1.8.0
	github.com/golang-jwt/jwt/v4 v4.5.2
	github.com/golang/gddo v0.0.0-20181116215533-9bd4a3295021
	github.com/golang/mock v1.6.0
	github.com/golang/snappy v1.0.0
	github.com/google/btree v1.1.3
	github.com/google/go-cmp v0.7.0
	github.com/google/go-jsonnet v0.17.0
	github.com/google/uuid v1.6.0
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/vault/api v1.0.2
	github.com/influxdata/cron v0.0.0-20201006132531-4bb0a200dcbe
	github.com/influxdata/flux v0.200.0
	github.com/influxdata/httprouter v1.3.1-0.20191122104820-ee83e2772f69
	github.com/influxdata/influx-cli/v2 v2.7.1-0.20250130214939-76d1c4d9b777
	github.com/influxdata/influxql v1.3.0
	github.com/influxdata/line-protocol v0.0.0-20200327222509-2487e7298839
	github.com/influxdata/pkg-config v0.3.0
	github.com/jmoiron/sqlx v1.3.4
	github.com/jsternberg/zap-logfmt v1.2.0
	github.com/jwilder/encoding v0.0.0-20170811194829-b4e1701a28ef
	github.com/kevinburke/go-bindata v3.22.0+incompatible
	github.com/mattn/go-isatty v0.0.20
	github.com/mattn/go-sqlite3 v1.14.18
	github.com/matttproud/golang_protobuf_extensions v1.0.4
	github.com/mileusna/useragent v0.0.0-20190129205925-3e331f0949a5
	github.com/mna/pigeon v1.0.1-0.20180808201053-bb0192cfc2ae
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.19.1
	github.com/prometheus/client_model v0.6.2
	github.com/prometheus/common v0.53.0
	github.com/retailnext/hllpp v1.0.1-0.20180308014038-101a6d2f8b52
	github.com/spf13/cast v1.3.0
	github.com/spf13/cobra v1.7.0
	github.com/spf13/pflag v1.0.6
	github.com/spf13/viper v1.6.1
	github.com/stretchr/testify v1.11.1
	github.com/testcontainers/testcontainers-go v0.18.0
	github.com/tinylib/msgp v1.1.0
	github.com/uber/jaeger-client-go v2.28.0+incompatible
	github.com/xlab/treeprint v1.0.0
	github.com/yudai/gojsondiff v1.0.0
	go.etcd.io/bbolt v1.3.6
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.27.0
	golang.org/x/crypto v0.48.0
	golang.org/x/exp v0.0.0-20250408133849-7e4ce0ab07d0
	golang.org/x/sync v0.19.0
	golang.org/x/sys v0.41.0
	golang.org/x/text v0.34.0
	golang.org/x/time v0.11.0
	golang.org/x/tools v0.41.0
	google.golang.org/protobuf v1.36.10
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
	honnef.co/go/tools v0.4.6
)

require (
	cel.dev/expr v0.25.1 // indirect
	cloud.google.com/go v0.121.0 // indirect
	cloud.google.com/go/auth v0.16.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/bigquery v1.66.2 // indirect
	cloud.google.com/go/bigtable v1.35.0 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	cloud.google.com/go/iam v1.5.0 // indirect
	cloud.google.com/go/longrunning v0.6.6 // indirect
	cloud.google.com/go/monitoring v1.24.0 // indirect
	filippo.io/edwards25519 v1.1.1 // indirect
	github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4 // indirect
	github.com/99designs/keyring v1.2.2 // indirect
	github.com/AlecAivazis/survey/v2 v2.3.4 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.21.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.13.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.11.2 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.3.2 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20210617225240-d185dfc1b5a1 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.6.0 // indirect
	github.com/DATA-DOG/go-sqlmock v1.4.1 // indirect
	github.com/Masterminds/semver v1.4.2 // indirect
	github.com/Masterminds/semver/v3 v3.3.1 // indirect
	github.com/Masterminds/sprig v2.16.0+incompatible // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/SAP/go-hdb v1.15.2 // indirect
	github.com/aokoli/goutils v1.0.1 // indirect
	github.com/apache/arrow/go/v15 v15.0.2 // indirect
	github.com/aws/aws-sdk-go-v2 v1.41.4 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.2 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.32.12 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.12 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.20 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.16.22 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/athena v1.57.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.3.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.20 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.17.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.54.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.9 // indirect
	github.com/aws/smithy-go v1.24.2 // indirect
	github.com/benbjohnson/immutable v0.4.3 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bonitoo-io/go-sql-bigquery v0.3.4-1.4.0 // indirect
	github.com/cenkalti/backoff/v4 v4.2.0 // indirect
	github.com/cncf/xds/go v0.0.0-20251210132809-ee656c7534f5 // indirect
	github.com/containerd/containerd v1.6.18 // indirect
	github.com/danieljoos/wincred v1.2.2 // indirect
	github.com/deepmap/oapi-codegen v1.6.0 // indirect
	github.com/docker/distribution v2.8.2+incompatible // indirect
	github.com/docker/docker v23.0.3+incompatible // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dvsekhvalnov/jose2go v1.7.0 // indirect
	github.com/eclipse/paho.mqtt.golang v1.5.1 // indirect
	github.com/editorconfig/editorconfig-core-go/v2 v2.1.1 // indirect
	github.com/elastic/go-sysinfo v1.8.1 // indirect
	github.com/elastic/go-windows v1.0.0 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.36.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.0 // indirect
	github.com/fatih/color v1.15.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.5.4 // indirect
	github.com/gabriel-vasile/mimetype v1.4.7 // indirect
	github.com/glycerine/go-unsnap-stream v0.0.0-20181221182339-f9677308dec2 // indirect
	github.com/glycerine/goconvey v0.0.0-20190410193231-58a59202ab31 // indirect
	github.com/go-crypt/x v0.3.2 // indirect
	github.com/go-jose/go-jose/v4 v4.1.3 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-sql-driver/mysql v1.9.3 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/godbus/dbus v0.0.0-20190726142602-4481cbc300e2 // indirect
	github.com/gofrs/uuid v3.3.0+incompatible // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/golang/geo v0.0.0-20190916061304-5b978397cfec // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/flatbuffers v25.9.23+incompatible // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.6 // indirect
	github.com/googleapis/gax-go/v2 v2.14.1 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/gsterjov/go-libsecret v0.0.0-20161001094733-a6f4afe4910c // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.1 // indirect
	github.com/hashicorp/go-hclog v0.12.2 // indirect
	github.com/hashicorp/go-retryablehttp v0.6.4 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/hashicorp/vault/sdk v0.1.8 // indirect
	github.com/huandu/xstrings v1.0.0 // indirect
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/influxdata/athenadriver/v2 v2.0.0 // indirect
	github.com/influxdata/gosnowflake/v2 v2.0.0 // indirect
	github.com/influxdata/influxdb-client-go/v2 v2.3.1-0.20210518120617-5d1fff431040 // indirect
	github.com/influxdata/line-protocol/v2 v2.2.1 // indirect
	github.com/influxdata/tdigest v0.0.2-0.20210216194612-fc98d27c9e8b // indirect
	github.com/influxdata/vertica-sql-go v1.3.5 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.8.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jedib0t/go-pretty/v6 v6.2.7 // indirect
	github.com/joeshaw/multierror v0.0.0-20140124173710-69b34d4ec901 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.11 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/mgutz/ansi v0.0.0-20170206155736-9520e82c474b // indirect
	github.com/microsoft/go-mssqldb v1.9.8 // indirect
	github.com/miekg/dns v1.1.29 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/moby/patternmatcher v0.5.0 // indirect
	github.com/moby/sys/sequential v0.5.0 // indirect
	github.com/moby/term v0.0.0-20221128092401-c43b287e0e0f // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/mtibben/percent v0.2.1 // indirect
	github.com/onsi/ginkgo v1.12.1 // indirect
	github.com/onsi/gomega v1.10.3 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.0-rc2 // indirect
	github.com/opencontainers/runc v1.1.5 // indirect
	github.com/pelletier/go-toml v1.9.5 // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pierrec/lz4 v2.0.5+incompatible // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/procfs v0.15.0 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/ryanuber/go-glob v1.0.0 // indirect
	github.com/segmentio/kafka-go v0.4.50 // indirect
	github.com/sergi/go-diff v1.1.0 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spf13/afero v1.10.0 // indirect
	github.com/spf13/jwalterweatherman v1.0.0 // indirect
	github.com/spiffe/go-spiffe/v2 v2.6.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/subosito/gotenv v1.2.0 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/uber-go/tally/v4 v4.1.17 // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/willf/bitset v1.1.11 // indirect
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2 // indirect
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82 // indirect
	github.com/yudai/pp v2.0.1+incompatible // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.60.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.60.0 // indirect
	go.opentelemetry.io/otel v1.40.0 // indirect
	go.opentelemetry.io/otel/metric v1.40.0 // indirect
	go.opentelemetry.io/otel/sdk v1.40.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.40.0 // indirect
	go.opentelemetry.io/otel/trace v1.40.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/exp/typeparams v0.0.0-20230321023759-10a507213a29 // indirect
	golang.org/x/mod v0.32.0 // indirect
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/oauth2 v0.34.0 // indirect
	golang.org/x/telemetry v0.0.0-20260109210033-bd525da824e2 // indirect
	golang.org/x/term v0.40.0 // indirect
	golang.org/x/tools/go/expect v0.1.1-deprecated // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	gonum.org/v1/gonum v0.16.0 // indirect
	google.golang.org/api v0.230.0 // indirect
	google.golang.org/genproto v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/grpc v1.79.3 // indirect
	gopkg.in/ini.v1 v1.51.0 // indirect
	gopkg.in/square/go-jose.v2 v2.5.1 // indirect
	howett.net/plist v0.0.0-20181124034731-591f970eefbb // indirect
)

replace github.com/nats-io/nats-streaming-server v0.11.2 => github.com/influxdata/nats-streaming-server v0.11.3-0.20201112040610-c277f7560803

replace google.golang.org/protobuf v1.30.0 => google.golang.org/protobuf v1.33.0
