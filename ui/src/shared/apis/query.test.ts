import {mocked} from 'ts-jest/utils'
import {runQuery} from 'src/shared/apis/query'

import {RunQuerySuccessResult} from 'src/shared/apis/query'
import {AppState} from 'src/types'

jest.mock('src/shared/apis/query')

const mockState = ({
  app: {
    persisted: {
      timeZone: 'Local',
    },
  },
  currentDashboard: {
    id: '',
  },
  resources: {
    variables: {
      byID: {
        '054b7476389f1000': {
          id: '054b7476389f1000',
          orgID: '674b23253171ee69',
          name: 'bucket',
          selected: ['Homeward Bound'],
          arguments: {
            type: 'query',
            values: {
              query:
                '// buckets\nbuckets()\n  |> filter(fn: (r) => r.name !~ /^_/)\n  |> rename(columns: {name: "_value"})\n  |> keep(columns: ["_value"])\n',
              language: 'flux',
            },
          },
        },
        '05782ef09ddb8000': {
          id: '05782ef09ddb8000',
          orgID: '674b23253171ee69',
          name: 'base_query',
          selected: [],
          arguments: {
            type: 'query',
            values: {
              query:
                '// base_query\nfrom(bucket: v.bucket)\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "cpu")\n  |> filter(fn: (r) => r._field == "usage_user")',
              language: 'flux',
            },
          },
        },
        '05aeb0ad75aca000': {
          id: '05aeb0ad75aca000',
          orgID: '674b23253171ee69',
          name: 'values',
          selected: ['system'],
          arguments: {
            type: 'map',
            values: {
              system: 'system',
              usage_user: 'usage_user',
            },
          },
        },
        '05ba3253105a5000': {
          id: '05ba3253105a5000',
          orgID: '674b23253171ee69',
          name: 'broker_host',
          selected: [],
          arguments: {
            type: 'query',
            values: {
              query:
                '// broker_host\nimport "influxdata/influxdb/v1"\nv1.tagValues(bucket: v.bucket, tag: "host")',
              language: 'flux',
            },
          },
        },
        '05e6e4df2287b000': {
          id: '05e6e4df2287b000',
          orgID: '674b23253171ee69',
          name: 'deployment',
          selected: [],
          arguments: {
            type: 'query',
            values: {
              query:
                '// deployment\nimport "influxdata/influxdb/v1"\nv1.tagValues(bucket: v.bucket, tag: "cpu") |> keep(columns: ["_value"])',
              language: 'flux',
            },
          },
        },
        '05e6e4fb0887b000': {
          id: '05e6e4fb0887b000',
          orgID: '674b23253171ee69',
          name: 'build',
          selected: [],
          arguments: {
            type: 'query',
            values: {
              query:
                '// build\nimport "influxdata/influxdb/v1"\nimport "strings"\n\nv1.tagValues(bucket: v.bucket, tag: "cpu") |> filter(fn: (r) => strings.hasSuffix(v: r._value, suffix: v.deployment))',
              language: 'flux',
            },
          },
        },
      },
      allIDs: [
        '054b7476389f1000',
        '05782ef09ddb8000',
        '05aeb0ad75aca000',
        '05ba3253105a5000',
        '05e6e4df2287b000',
        '05e6e4fb0887b000',
      ],
    },
  },
} as unknown) as AppState

describe('query', () => {
  const filePath = 'src/shared/apis/query'
  describe('runQuery', () => {
    beforeEach(() => {
      const {resetQueryCache} = require('src/shared/apis/queryCache')
      // NOTE: as long as you mock children like below, before importing your
      // component by using a require().default pattern, this will reset your
      // mocks between tests (alex)
      jest.clearAllMocks()
      resetQueryCache()
    })

    it('calls runQuery when there is no matching query in the cache & returns cached results when an unexpired match is found', done => {
      const promise = new Promise((res, rej) => {
        return res({
          type: 'SUCCESS',
          csv: 'wooooo',
          didTruncate: true,
          bytesRead: 1,
        } as RunQuerySuccessResult)
      })
      // returns a mock runQuery
      mocked(runQuery).mockImplementation(() => ({
        promise,
        cancel: jest.fn(),
      }))
      const {getRunQueryResults} = require('src/shared/apis/queryCache')
      const orgID = 'orgID'
      const queryText = '|> get some data fool'
      const result = getRunQueryResults(orgID, queryText, mockState)
      expect(runQuery).toHaveBeenCalledTimes(1)
      result.promise.then(() => {
        try {
          // expect runQuery to have still only be called once
          // expect results to be retrieved from cache
          getRunQueryResults(orgID, queryText, mockState)
          expect(runQuery).toHaveBeenCalledTimes(1)
          done()
        } catch (error) {
          done(error)
        }
      })
    })

    it('clears the cache by queryText', done => {
      const promise = new Promise((res, rej) => {
        return res({
          type: 'SUCCESS',
          csv: 'wooooo',
          didTruncate: true,
          bytesRead: 1,
        } as RunQuerySuccessResult)
      })
      mocked(runQuery).mockImplementation(() => ({
        promise,
        cancel: jest.fn(),
      }))
      const {
        getRunQueryResults,
        resetQueryCacheByQuery,
      } = require('src/shared/apis/queryCache')
      const orgID = 'orgID'
      const queryText = '|> get some data fool'
      const result = getRunQueryResults(orgID, queryText, mockState)
      expect(runQuery).toHaveBeenCalledTimes(1)
      result.promise.then(() => {
        try {
          // expect the cache to be cleared
          resetQueryCacheByQuery(queryText)
          // expect results to be retrieved from runQuery
          getRunQueryResults(orgID, queryText, mockState)
          expect(runQuery).toHaveBeenCalledTimes(2)
          done()
        } catch (error) {
          done(error)
        }
      })
    })

    it('invalidates the cached results after the time invalidation constant', done => {
      const promise = new Promise((res, rej) => {
        return res({
          type: 'SUCCESS',
          csv: 'wooooo',
          didTruncate: true,
          bytesRead: 1,
        } as RunQuerySuccessResult)
      })
      mocked(runQuery).mockImplementation(() => ({
        promise,
        cancel: jest.fn(),
      }))
      const {
        getRunQueryResults,
        TIME_INVALIDATION,
      } = require('src/shared/apis/queryCache')
      const orgID = 'orgID'
      const queryText = '|> get some data fool'
      getRunQueryResults(orgID, queryText, mockState)
      expect(runQuery).toHaveBeenCalledTimes(1)
      setTimeout(() => {
        try {
          getRunQueryResults(orgID, queryText, mockState)
          expect(runQuery).toHaveBeenCalledTimes(2)
          done()
        } catch (error) {
          done(error)
        }
        // wait 5100 seconds
      }, TIME_INVALIDATION + 100)
    }, 6000)
    it('returns the cached results when an unexpired match with the same variable is found', done => {
      const promise = new Promise((res, rej) => {
        return res({
          type: 'SUCCESS',
          csv: 'wooooo',
          didTruncate: true,
          bytesRead: 1,
        } as RunQuerySuccessResult)
      })
      mocked(runQuery).mockImplementation(() => ({
        promise,
        cancel: jest.fn(),
      }))
      const {getRunQueryResults} = require('src/shared/apis/queryCache')
      const orgID = 'orgID'
      const queryText = 'v.bucket'
      const result = getRunQueryResults(orgID, queryText, mockState)
      expect(runQuery).toHaveBeenCalledTimes(1)
      result.promise.then(() => {
        try {
          // expect runQuery to have still only be called once
          // expect results to be retrieved from cache
          getRunQueryResults(orgID, queryText, mockState)
          expect(runQuery).toHaveBeenCalledTimes(1)
          done()
        } catch (error) {
          done(error)
        }
      })
    })
    it('resets the matching query if the variables do not match and reruns the query', done => {
      const promise = new Promise((res, rej) => {
        return res({
          type: 'SUCCESS',
          csv: 'wooooo',
          didTruncate: true,
          bytesRead: 1,
        } as RunQuerySuccessResult)
      })
      mocked(runQuery).mockImplementation(() => ({
        promise,
        cancel: jest.fn(),
      }))
      const {getRunQueryResults} = require('src/shared/apis/queryCache')
      const orgID = 'orgID'
      const queryText = 'v.build'
      const originalName =
        mockState.resources.variables.byID['05e6e4df2287b000'].name
      const result = getRunQueryResults(orgID, queryText, mockState)
      expect(runQuery).toHaveBeenCalledTimes(1)
      result.promise
        .then(() => {
          try {
            const newMockState = Object.assign({}, mockState)
            newMockState.resources.variables.byID['05e6e4df2287b000'].name =
              'newName'
            getRunQueryResults(orgID, queryText, newMockState)
            expect(runQuery).toHaveBeenCalledTimes(2)
          } catch (error) {
            done(error)
          }
        })
        .then(() => {
          try {
            mockState.resources.variables.byID[
              '05e6e4df2287b000'
            ].name = originalName
            getRunQueryResults(orgID, queryText, mockState)
            expect(runQuery).toHaveBeenCalledTimes(3)
            done()
          } catch (error) {
            done(error)
          }
        })
    })
    it('resets the matching query if the selected variables do not match and reruns the query', done => {
      const promise = new Promise((res, rej) => {
        return res({
          type: 'SUCCESS',
          csv: 'wooooo',
          didTruncate: true,
          bytesRead: 1,
        } as RunQuerySuccessResult)
      })
      mocked(runQuery).mockImplementation(() => ({
        promise,
        cancel: jest.fn(),
      }))
      const {getRunQueryResults} = require('src/shared/apis/queryCache')
      const orgID = 'orgID'
      const queryText = 'v.values'
      const [selected] = mockState.resources.variables.byID[
        '05aeb0ad75aca000'
      ].selected
      const result = getRunQueryResults(orgID, queryText, mockState)
      expect(runQuery).toHaveBeenCalledTimes(1)
      result.promise
        .then(() => {
          try {
            const newMockState = Object.assign({}, mockState)
            newMockState.resources.variables.byID[
              '05aeb0ad75aca000'
            ].selected[0] = 'usage_user'
            getRunQueryResults(orgID, queryText, newMockState)
            expect(runQuery).toHaveBeenCalledTimes(2)
          } catch (error) {
            done(error)
          }
        })
        .then(() => {
          try {
            mockState.resources.variables.byID[
              '05aeb0ad75aca000'
            ].selected[0] = selected
            getRunQueryResults(orgID, queryText, mockState)
            expect(runQuery).toHaveBeenCalledTimes(3)
            done()
          } catch (error) {
            done(error)
          }
        })
    })
    it('returns cached results even when variables irrelevant to a query are toggled', done => {
      const promise = new Promise((res, rej) => {
        return res({
          type: 'SUCCESS',
          csv: 'wooooo',
          didTruncate: true,
          bytesRead: 1,
        } as RunQuerySuccessResult)
      })
      mocked(runQuery).mockImplementation(() => ({
        promise,
        cancel: jest.fn(),
      }))
      const {getRunQueryResults} = require('src/shared/apis/queryCache')
      const orgID = 'orgID'
      const queryText = 'v.bucket'
      const result = getRunQueryResults(orgID, queryText, mockState)
      expect(runQuery).toHaveBeenCalledTimes(1)
      result.promise.then(() => {
        try {
          mockState.resources.variables.byID['05aeb0ad75aca000'].selected[0] =
            'usage_user'
          getRunQueryResults(orgID, queryText, mockState)
          expect(runQuery).toHaveBeenCalledTimes(1)
          done()
        } catch (error) {
          done(error)
        }
      })
    })
  })
})
