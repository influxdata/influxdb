import {filterUnusedVarsBasedOnQuery} from 'src/shared/utils/filterUnusedVars'
import {RemoteDataState, Variable} from 'src/types'

const bucketVariable: Variable = {
  id: '054b7476389f1000',
  orgID: '674b23253171ee69',
  name: 'bucket',
  description: '',
  selected: ['Homeward Bound'],
  arguments: {
    type: 'query',
    values: {
      query:
        '// buckets\nbuckets()\n  |> filter(fn: (r) => r.name !~ /^_/)\n  |> rename(columns: {name: "_value"})\n  |> keep(columns: ["_value"])\n',
      language: 'flux',
      results: ['Futile Devices', 'Homeward Bound', 'damn'],
    },
  },
  createdAt: '2020-02-25T11:30:40.482278-08:00',
  updatedAt: '2020-07-15T14:28:39.240725-07:00',
  labels: [],
  links: {
    self: '/api/v2/variables/054b7476389f1000',
    labels: '/api/v2/variables/054b7476389f1000/labels',
    org: '/api/v2/orgs/674b23253171ee69',
  },
  status: RemoteDataState.Done,
}

const deploymentVariable: Variable = {
  id: '05e6e4df2287b000',
  orgID: '674b23253171ee69',
  name: 'deployment',
  description: '',
  selected: [],
  arguments: {
    type: 'query',
    values: {
      query:
        '// deployment\nimport "influxdata/influxdb/v1"\nv1.tagValues(bucket: v.bucket, tag: "cpu") |> keep(columns: ["_value"])',
      language: 'flux',
      results: [],
    },
  },
  createdAt: '2020-06-25T06:06:21.962137-07:00',
  updatedAt: '2020-07-15T14:28:55.499456-07:00',
  labels: [],
  links: {
    self: '/api/v2/variables/05e6e4df2287b000',
    labels: '/api/v2/variables/05e6e4df2287b000/labels',
    org: '/api/v2/orgs/674b23253171ee69',
  },
  status: RemoteDataState.Done,
}

const buildVariable: Variable = {
  id: '05e6e4fb0887b000',
  orgID: '674b23253171ee69',
  name: 'build',
  description: '',
  selected: [],
  arguments: {
    type: 'query',
    values: {
      query:
        '// build\nimport "influxdata/influxdb/v1"\nimport "strings"\n\nv1.tagValues(bucket: v.bucket, tag: "cpu") |> filter(fn: (r) => strings.hasSuffix(v: r._value, suffix: v.deployment))',
      language: 'flux',
      results: [],
    },
  },
  createdAt: '2020-06-25T06:06:50.530959-07:00',
  updatedAt: '2020-07-15T14:28:48.134806-07:00',
  labels: [],
  links: {
    self: '/api/v2/variables/05e6e4fb0887b000',
    labels: '/api/v2/variables/05e6e4fb0887b000/labels',
    org: '/api/v2/orgs/674b23253171ee69',
  },
  status: RemoteDataState.Done,
}

const variables: Variable[] = [
  bucketVariable,
  {
    id: '05782ef09ddb8000',
    orgID: '674b23253171ee69',
    name: 'base_query',
    description: '',
    selected: [],
    arguments: {
      type: 'query',
      values: {
        query:
          '// base_query\nfrom(bucket: v.bucket)\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "cpu")\n  |> filter(fn: (r) => r._field == "usage_user")',
        language: 'flux',
        results: [],
      },
    },
    createdAt: '2020-03-31T06:18:34.615076-07:00',
    updatedAt: '2020-07-15T14:28:26.685766-07:00',
    labels: [],
    links: {
      self: '/api/v2/variables/05782ef09ddb8000',
      labels: '/api/v2/variables/05782ef09ddb8000/labels',
      org: '/api/v2/orgs/674b23253171ee69',
    },
    status: RemoteDataState.Done,
  },
  {
    id: '05aeb0ad75aca000',
    orgID: '674b23253171ee69',
    name: 'values',
    description: '',
    selected: ['system'],
    arguments: {
      type: 'map',
      values: {system: 'system', usage_user: 'usage_user'},
    },
    createdAt: '2020-05-12T14:23:23.222747-07:00',
    updatedAt: '2020-05-12T14:23:23.222747-07:00',
    labels: [],
    links: {
      self: '/api/v2/variables/05aeb0ad75aca000',
      labels: '/api/v2/variables/05aeb0ad75aca000/labels',
      org: '/api/v2/orgs/674b23253171ee69',
    },
    status: RemoteDataState.Done,
  },
  {
    id: '05ba3253105a5000',
    orgID: '674b23253171ee69',
    name: 'broker_host',
    description: '',
    selected: [],
    arguments: {
      type: 'query',
      values: {
        query:
          '// broker_host\nimport "influxdata/influxdb/v1"\nv1.tagValues(bucket: v.bucket, tag: "host")',
        language: 'flux',
        results: [],
      },
    },
    createdAt: '2020-05-21T12:53:06.881887-07:00',
    updatedAt: '2020-07-15T14:28:33.980146-07:00',
    labels: [],
    links: {
      self: '/api/v2/variables/05ba3253105a5000',
      labels: '/api/v2/variables/05ba3253105a5000/labels',
      org: '/api/v2/orgs/674b23253171ee69',
    },
    status: RemoteDataState.Done,
  },
  deploymentVariable,
  buildVariable,
]

describe('filterUnusedVars', () => {
  describe('filterUnusedVarsBasedOnQuery', () => {
    it('returns an empty array when no variables or queries are passed', () => {
      const actual = filterUnusedVarsBasedOnQuery([], [])
      expect(actual).toEqual([])
    })
    it('returns an empty array when no query is passed', () => {
      const actual = filterUnusedVarsBasedOnQuery(variables, [])
      expect(actual).toEqual([])
    })
    it("returns an empty array when the query doesn't contain a variable", () => {
      const actual = filterUnusedVarsBasedOnQuery(variables, ['random query'])
      expect(actual).toEqual([])
    })
    it('returns a variable when it exists in the query and does not depend on another query', () => {
      const actual = filterUnusedVarsBasedOnQuery(variables, ['v.bucket'])
      expect(actual).toEqual([bucketVariable])
    })
    it('returns a variable and its dependent variables when dependencies exist', () => {
      const actual = filterUnusedVarsBasedOnQuery(variables, ['v.deployment'])
      expect(actual).toEqual([deploymentVariable, bucketVariable])
    })
    it('returns a variable and its deeply nested dependent variables', () => {
      const actual = filterUnusedVarsBasedOnQuery(variables, ['v.build'])
      expect(actual).toEqual([
        buildVariable,
        bucketVariable,
        deploymentVariable,
      ])
    })
  })
})
