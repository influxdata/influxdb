import {createStore} from 'redux'

import {
  buildActiveQuery,
  initialState,
  initialStateHelper,
  timeMachinesReducer,
} from 'src/timeMachine/reducers'

describe('the Time Machine reducer', () => {
  describe('setting the default aggregateFunctionType', () => {
    const store = createStore(timeMachinesReducer, initialState())
    const expectedAggregatefunctionType = initialStateHelper().queryBuilder
      .tags[0].aggregateFunctionType

    it('is set when setting a builder bucket selection', () => {
      store.dispatch({
        type: 'SET_BUILDER_BUCKET_SELECTION',
        payload: {bucket: 'foo', resetSelections: true},
      })
      const actualState = store.getState()
      const defaultAggregateFunctionType =
        actualState.timeMachines.de.draftQueries[0].builderConfig.tags[0]
          .aggregateFunctionType

      expect(defaultAggregateFunctionType).toEqual(
        expectedAggregatefunctionType
      )
    })

    it('is set when adding a new tag selector', () => {
      store.dispatch({type: 'ADD_TAG_SELECTOR'})
      const actualState = store.getState()
      const defaultAggregateFunctionType =
        actualState.timeMachines.de.draftQueries[0].builderConfig.tags[0]
          .aggregateFunctionType

      expect(defaultAggregateFunctionType).toEqual(
        expectedAggregatefunctionType
      )
    })
  })

  describe('buildActiveQuery', () => {
    let draftState

    beforeEach(() => {
      draftState = initialStateHelper()
    })

    it('builds the query if the query builder config is valid', () => {
      const expectedText =
        'from(bucket: "metrics")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "mem")\n  |> filter(fn: (r) => r._field == "active")\n  |> aggregateWindow(every: v.windowPeriod, fn: mean)\n  |> yield(name: "mean")'
      const validDraftQuery = {
        text: '',
        editMode: 'advanced',
        name: '',
        builderConfig: {
          buckets: ['metrics'],
          tags: [
            {
              key: '_measurement',
              values: ['mem'],
              aggregateFunctionType: 'filter',
            },
            {
              key: '_field',
              values: ['active'],
              aggregateFunctionType: 'filter',
            },
            {
              key: 'host',
              values: [],
              aggregateFunctionType: 'filter',
            },
          ],
          functions: [
            {
              name: 'mean',
            },
          ],
          aggregateWindow: {
            period: 'auto',
          },
        },
        hidden: false,
      }

      draftState.draftQueries[draftState.activeQueryIndex] = validDraftQuery
      buildActiveQuery(draftState)
      expect(draftState.draftQueries[draftState.activeQueryIndex].text).toEqual(
        expectedText
      )
    })

    it("sets the text of invalid queries to empty string if they don't have any query text", () => {
      const invalidDraftQueryWithoutText = {
        editMode: 'advanced',
        name: '',
        builderConfig: {
          buckets: [],
          tags: [
            {
              key: '_measurement',
              values: [],
            },
          ],
          functions: [],
          aggregateWindow: {
            period: '',
          },
        },
        hidden: false,
      }

      draftState.draftQueries[
        draftState.activeQueryIndex
      ] = invalidDraftQueryWithoutText
      buildActiveQuery(draftState)
      expect(draftState.draftQueries[draftState.activeQueryIndex].text).toEqual(
        ''
      )
    })

    it("retains text of valid queries that weren't built with the query builder", () => {
      const invalidDraftQueryWithText = {
        text:
          'from(bucket: v.bucket)\n  |> range(start: v.timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "system")\n  |> filter(fn: (r) => r._field == "load1" or r._field == "load5" or r._field == "load15")\n  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)\n  |> yield(name: "mean")',
        editMode: 'advanced',
        name: '',
        builderConfig: {
          buckets: [],
          tags: [
            {
              key: '_measurement',
              values: [],
            },
          ],
          functions: [],
          aggregateWindow: {
            period: '',
          },
        },
        hidden: false,
      }

      draftState.draftQueries[
        draftState.activeQueryIndex
      ] = invalidDraftQueryWithText
      buildActiveQuery(draftState)
      expect(draftState.draftQueries[draftState.activeQueryIndex].text).toEqual(
        draftState.draftQueries[draftState.activeQueryIndex].text
      )
    })
  })
})
