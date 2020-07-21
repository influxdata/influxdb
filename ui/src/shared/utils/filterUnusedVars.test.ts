import {filterUnusedVarsBasedOnQuery} from 'src/shared/utils/filterUnusedVars'
import {RemoteDataState, Variable} from 'src/types'

const bucketVariable = {
  id: '054b7476389f1000',
  name: 'bucket',
  selected: ['Homeward Bound'],
  arguments: {
    type: 'query',
    values: {
      query:
        '// buckets\nbuckets()\n  |> filter(fn: (r) => r.name !~ /^_/)\n  |> rename(columns: {name: "_value"})\n  |> keep(columns: ["_value"])\n',
      language: 'flux',
      results: ['Futile Devices', 'Homeward Bound', 'woo'],
    },
  },
} as Variable

const deploymentVariable = {
  id: '05e6e4df2287b000',
  name: 'deployment',
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
} as Variable

const buildVariable = {
  id: '05e6e4fb0887b000',
  name: 'build',
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
} as Variable

const variables = [
  bucketVariable,
  {
    id: '05782ef09ddb8000',
    name: 'base_query',
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
  },
  {
    id: '05aeb0ad75aca000',
    name: 'values',
    selected: ['system'],
    arguments: {
      type: 'map',
      values: {system: 'system', usage_user: 'usage_user'},
    },
  },
  {
    id: '05ba3253105a5000',
    name: 'broker_host',
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
  },
  deploymentVariable,
  buildVariable,
] as Variable[]

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
