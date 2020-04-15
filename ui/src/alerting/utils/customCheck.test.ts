import {createCheckQueryFromAlertBuilder} from 'src/alerting/utils/customCheck'
import {BuilderConfig, RemoteDataState} from 'src/types'
import {AlertBuilderState} from '../reducers/alertBuilder'

const bc1: BuilderConfig = {
  buckets: ['bestBuck'],
  tags: [
    {key: 'k1', values: ['v1'], aggregateFunctionType: 'filter'},
    {key: '_field', values: ['v2'], aggregateFunctionType: 'filter'},
  ],
  functions: [{name: 'mean'}],
}

const ab1: AlertBuilderState = {
  activeStatus: 'active',
  status: RemoteDataState.Done,
  statusMessageTemplate: 'this is staus message',
  tags: [{key: 'k1', value: 'v1'}],
  id: '2',
  name: 'name of thing',
  every: '2d',
  offset: '10m',
  type: 'deadman',
  staleTime: 'lala',
  level: 'INFO',
  timeSince: '10m',
  reportZero: false,
  thresholds: [
    {type: 'greater', value: 45},
    {type: 'lesser', value: 15},
    {type: 'range', min: 2, max: 10, within: false},
  ],
}

const ab2: AlertBuilderState = {
  activeStatus: 'active',
  status: RemoteDataState.Done,
  statusMessageTemplate: 'this is staus message',
  tags: [{key: 'k1', value: 'v1'}],
  id: '2',
  name: 'name of thing',
  every: '2d',
  offset: '10m',
  type: 'threshold',
  staleTime: 'lala',
  level: 'INFO',
  timeSince: '10m',
  reportZero: false,
  thresholds: [
    {type: 'greater', value: 45, level: 'INFO'},
    {type: 'lesser', value: 15, level: 'OK'},
    {type: 'range', min: 2, max: 10, within: false, level: 'WARN'},
  ],
}

const TESTS = [
  [
    bc1,
    ab1,
    'package main\nimport "influxdata/influxdb/monitor"\nimport "experimental"\nimport "influxdata/influxdb/v1"\n\ncheck = {\n  _check_id: "2",\n  _check_name: "name of thing",\n  _type: "custom",\n  tags: {k1: "v1"},\n  every: 2d\n}\n\noption task = {\n  name: "name of thing",\n  every: 2d, // expected to match check.every\n  offset: 10m\n}\n\ninfo = (r) => (r.dead)\n\nmessageFn = (r) =>("this is staus message")\n\ndata = from(bucket: "bestBuck")\n  |> range(start: -lala)\n  |> filter(fn: (r) => r.k1 == "v1")\n  |> filter(fn: (r) => r._field == "v2")\n\ndata\n  |> v1.fieldsAsCols()\n  |> monitor.deadman(t: experimental.subDuration(from: now(), d: 10m))\n  |> monitor.check(data: check, messageFn: messageFn,info:info)',
  ],
  [
    bc1,
    ab2,
    'package main\nimport "influxdata/influxdb/monitor"\nimport "influxdata/influxdb/v1"\n\ncheck = {\n  _check_id: "2",\n  _check_name: "name of thing",\n  _type: "custom",\n  tags: {k1: "v1"},\n  every: 2d\n}\n\noption task = {\n  name: "name of thing",\n  every: 2d, // expected to match check.every\n  offset: 10m\n}\n\ninfo = (r) =>(r.v2 > 45)\nok = (r) =>(r.v2 < 15)\nwarn = (r) =>(r.v2 < 2 and r.v2 > 10)\n\nmessageFn = (r) =>("this is staus message")\n\ndata = from(bucket: "bestBuck")\n  |> range(start: -check.every)\n  |> filter(fn: (r) => r.k1 == "v1")\n  |> filter(fn: (r) => r._field == "v2")\n  |> aggregateWindow(every: check.every, fn: mean, createEmpty: false)\n\ndata\n  |> v1.fieldsAsCols()\n  |> monitor.check(data: check, messageFn: messageFn, info:info, ok:ok, warn:warn)',
  ],
]

test.each(TESTS)(
  'createCheckQueryFromAlertBuilder',
  (builderConfig, alertBuilder, expected) => {
    expect(
      createCheckQueryFromAlertBuilder(builderConfig, alertBuilder)
    ).toEqual(expected)
  }
)
