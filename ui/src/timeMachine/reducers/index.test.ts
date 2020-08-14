import {createStore} from 'redux'

import {
  buildActiveQuery,
  initialState,
  initialStateHelper,
  timeMachinesReducer,
  timeMachineReducer,
} from 'src/timeMachine/reducers'
import {setBuilderTagKeysStatus} from 'src/timeMachine/actions/queryBuilder'

import {RemoteDataState, TableViewProperties} from 'src/types'

describe('the Time Machine reducer', () => {
  describe('setting the default aggregateFunctionType', () => {
    const store = createStore(timeMachinesReducer, initialState())
    const expectedAggregateFunctionType = initialStateHelper().queryBuilder
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
        expectedAggregateFunctionType
      )
    })

    it('is set when adding a new tag selector', () => {
      store.dispatch({type: 'ADD_TAG_SELECTOR'})
      const actualState = store.getState()
      const defaultAggregateFunctionType =
        actualState.timeMachines.de.draftQueries[0].builderConfig.tags[0]
          .aggregateFunctionType

      expect(defaultAggregateFunctionType).toEqual(
        expectedAggregateFunctionType
      )
    })
  })

  describe('setBuilderTagKeyStatus', () => {
    it('sets tagValues "status" to "NotStarted" when tagKeys are "Loading"', () => {
      const {Loading, NotStarted} = RemoteDataState
      const dataExplorer = initialState().timeMachines.de
      dataExplorer.queryBuilder.tags[0].keysStatus = RemoteDataState.Done
      dataExplorer.queryBuilder.tags[0].valuesStatus = RemoteDataState.Done

      const state = timeMachineReducer(
        dataExplorer,
        setBuilderTagKeysStatus(0, Loading)
      )

      expect(state.queryBuilder.tags[0].keysStatus).toBe(Loading)
      expect(state.queryBuilder.tags[0].valuesStatus).toBe(NotStarted)
    })
  })

  describe('buildActiveQuery', () => {
    let draftState

    beforeEach(() => {
      draftState = initialStateHelper()
    })

    it('builds the query if the query builder config is valid', () => {
      const expectedText =
        'from(bucket: "metrics")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r["_measurement"] == "mem")\n  |> filter(fn: (r) => r["_field"] == "active")\n  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)\n  |> yield(name: "mean")'
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
            fillValues: false,
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
          'from(bucket: v.bucket)\n  |> range(start: v.timeRangeStart)\n  |> filter(fn: (r) => r["_measurement"] == "system")\n  |> filter(fn: (r) => r["_field"] == "load1" or r["_field"] == "load5" or r["_field"] == "load15")\n  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)\n  |> yield(name: "mean")',
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

  // Assertion of bugfix for https://github.com/influxdata/influxdb/issues/16426
  describe('setting the active query index', () => {
    const store = createStore(timeMachinesReducer, initialState())
    store.dispatch({type: 'ADD_QUERY'})
    store.dispatch({type: 'ADD_QUERY'})

    it('sets the activeQueryIndex when the value is less than the length of draftQueries', () => {
      const activeQueryIndex = 2
      store.dispatch({
        type: 'SET_ACTIVE_QUERY_INDEX',
        payload: {activeQueryIndex},
      })

      const actualState = store.getState()
      const activeTimeMachine =
        actualState.timeMachines[actualState.activeTimeMachineID]
      expect(activeTimeMachine.activeQueryIndex).toBe(activeQueryIndex)
    })

    it('does not set the activeQueryIndex when the value is greater than the length of draftQueries', () => {
      const activeQueryIndex = 5
      const originalActiveQueryIndex = store.getState().timeMachines[
        store.getState().activeTimeMachineID
      ].activeQueryIndex
      store.dispatch({
        type: 'SET_ACTIVE_QUERY_INDEX',
        payload: {activeQueryIndex},
      })

      const actualState = store.getState()
      const activeTimeMachine =
        actualState.timeMachines[actualState.activeTimeMachineID]
      expect(activeTimeMachine.activeQueryIndex).toBe(originalActiveQueryIndex)
    })
  })

  // Fix for: https://github.com/influxdata/influxdb/issues/17364
  describe('editing a table view', () => {
    it('does not overwrite internal TableViewProperites when files is an empty array', () => {
      const initial = initialState()

      const store = createStore(timeMachinesReducer, initial)
      store.dispatch({type: 'SET_VIEW_TYPE', payload: {type: 'table'}})

      const midState = store.getState()
      const tableViewProperties = midState.timeMachines[
        midState.activeTimeMachineID
      ].view.properties as TableViewProperties
      tableViewProperties.fieldOptions = [
        {internalName: '_foo', displayName: 'foo'},
      ]

      store.dispatch({
        type: 'SET_QUERY_RESULTS',
        payload: {
          status: RemoteDataState.Done,
          files: [],
          fetchDuration: 234,
        },
      })

      const endState = store.getState()
      expect(
        (endState.timeMachines[endState.activeTimeMachineID].view
          .properties as TableViewProperties).fieldOptions[0].displayName
      ).toBe(tableViewProperties.fieldOptions[0].displayName)
    })
  })
})
