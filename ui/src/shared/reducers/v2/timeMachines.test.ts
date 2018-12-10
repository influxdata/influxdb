// Reducers
import {
  initialState,
  initialStateHelper,
  timeMachineReducer,
  timeMachinesReducer,
} from 'src/shared/reducers/v2/timeMachines'

// Actions
import {
  submitScript,
  setQuerySource,
  setActiveTab,
  setActiveTimeMachine,
  setActiveQueryIndex,
  editActiveQueryWithBuilder,
  editActiveQueryAsFlux,
  editActiveQueryAsInfluxQL,
  addQuery,
  removeQuery,
  updateActiveQueryName,
} from 'src/shared/actions/v2/timeMachines'

// Utils
import {createView} from 'src/shared/utils/view'

// Constants
import {
  DE_TIME_MACHINE_ID,
  VEO_TIME_MACHINE_ID,
} from 'src/shared/constants/timeMachine'

// Types
import {TimeMachineTab} from 'src/types/v2/timeMachine'
import {
  DashboardQuery,
  QueryViewProperties,
  InfluxLanguage,
  QueryEditMode,
} from 'src/types/v2/dashboards'

describe('timeMachinesReducer', () => {
  test('it directs actions to the currently active timeMachine', () => {
    const state = initialState()
    const de = state.timeMachines[DE_TIME_MACHINE_ID]
    const veo = state.timeMachines[VEO_TIME_MACHINE_ID]

    expect(state.activeTimeMachineID).toEqual(DE_TIME_MACHINE_ID)
    expect(de.activeTab).toEqual(TimeMachineTab.Queries)
    expect(veo.activeTab).toEqual(TimeMachineTab.Queries)

    const nextState = timeMachinesReducer(
      state,
      setActiveTab(TimeMachineTab.Visualization)
    )

    const nextDE = nextState.timeMachines[DE_TIME_MACHINE_ID]
    const nextVEO = nextState.timeMachines[VEO_TIME_MACHINE_ID]

    expect(nextDE.activeTab).toEqual(TimeMachineTab.Visualization)
    expect(nextVEO.activeTab).toEqual(TimeMachineTab.Queries)
  })

  test('it resets tab and draftScript state on a timeMachine when activated', () => {
    const state = initialState()

    expect(state.activeTimeMachineID).toEqual(DE_TIME_MACHINE_ID)

    const activeTimeMachine = state.timeMachines[state.activeTimeMachineID]

    activeTimeMachine.activeQueryIndex = 2

    const view = createView<QueryViewProperties>()

    view.properties.queries = [
      {
        text: 'foo',
        type: InfluxLanguage.InfluxQL,
        sourceID: '123',
        editMode: QueryEditMode.Advanced,
        builderConfig: {
          buckets: [],
          measurements: [],
          fields: [],
          functions: [],
        },
      },
      {
        text: 'bar',
        type: InfluxLanguage.Flux,
        sourceID: '456',
        editMode: QueryEditMode.Builder,
        builderConfig: {
          buckets: [],
          measurements: [],
          fields: [],
          functions: [],
        },
      },
    ]

    const nextState = timeMachinesReducer(
      state,
      setActiveTimeMachine(VEO_TIME_MACHINE_ID, {view})
    )

    expect(nextState.activeTimeMachineID).toEqual(VEO_TIME_MACHINE_ID)

    const nextTimeMachine =
      nextState.timeMachines[nextState.activeTimeMachineID]

    expect(nextTimeMachine.activeTab).toEqual(TimeMachineTab.Queries)
    expect(nextTimeMachine.activeQueryIndex).toEqual(0)
    expect(nextTimeMachine.draftQueries).toEqual(view.properties.queries)
  })
})

describe('timeMachineReducer', () => {
  describe('SUBMIT_SCRIPT', () => {
    test('replaces each queries text', () => {
      const state = initialStateHelper()

      const queryA: DashboardQuery = {
        text: 'foo',
        type: InfluxLanguage.Flux,
        sourceID: '123',
        editMode: QueryEditMode.Builder,
        builderConfig: {
          buckets: [],
          measurements: [],
          fields: [],
          functions: [],
        },
      }

      const queryB: DashboardQuery = {
        text: 'bar',
        type: InfluxLanguage.Flux,
        sourceID: '456',
        editMode: QueryEditMode.Builder,
        builderConfig: {
          buckets: [],
          measurements: [],
          fields: [],
          functions: [],
        },
      }

      state.view.properties.queries = [queryA, queryB]
      state.draftQueries = [{...queryA, text: 'baz'}, {...queryB, text: 'buzz'}]

      const actual = timeMachineReducer(state, submitScript()).view.properties
        .queries

      const expected = state.draftQueries

      expect(actual).toEqual(expected)
    })
  })

  describe('SET_QUERY_SOURCE', () => {
    test('replaces the sourceID for the active query', () => {
      const state = initialStateHelper()

      expect(state.draftQueries[0].sourceID).toEqual('')

      const nextState = timeMachineReducer(state, setQuerySource('howdy'))

      expect(nextState.draftQueries[0].sourceID).toEqual('howdy')
    })
  })

  describe('EDIT_ACTIVE_QUERY_WITH_BUILDER', () => {
    test('changes the activeQueryEditor and editMode for the currently active query', () => {
      const state = initialStateHelper()

      state.activeQueryIndex = 1
      state.draftQueries = [
        {
          text: 'foo',
          type: InfluxLanguage.Flux,
          sourceID: '',
          editMode: QueryEditMode.Builder,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
        {
          text: 'bar',
          type: InfluxLanguage.Flux,
          sourceID: '',
          editMode: QueryEditMode.Advanced,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
      ]

      const nextState = timeMachineReducer(state, editActiveQueryWithBuilder())

      expect(nextState.activeQueryIndex).toEqual(1)
      expect(nextState.draftQueries).toEqual([
        {
          text: 'foo',
          type: InfluxLanguage.Flux,
          sourceID: '',
          editMode: QueryEditMode.Builder,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
        {
          text: '',
          type: InfluxLanguage.Flux,
          sourceID: '',
          editMode: QueryEditMode.Builder,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
      ])
    })
  })

  describe('EDIT_ACTIVE_QUERY_AS_FLUX', () => {
    test('changes the activeQueryEditor and editMode for the currently active query', () => {
      const state = initialStateHelper()

      state.activeQueryIndex = 1
      state.draftQueries = [
        {
          text: 'foo',
          type: InfluxLanguage.InfluxQL,
          sourceID: '',
          editMode: QueryEditMode.Advanced,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
        {
          text: 'bar',
          type: InfluxLanguage.Flux,
          sourceID: '',
          editMode: QueryEditMode.Builder,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
      ]

      const nextState = timeMachineReducer(state, editActiveQueryAsFlux())

      expect(nextState.activeQueryIndex).toEqual(1)
      expect(nextState.draftQueries).toEqual([
        {
          text: 'foo',
          type: InfluxLanguage.InfluxQL,
          sourceID: '',
          editMode: QueryEditMode.Advanced,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
        {
          text: 'bar',
          type: InfluxLanguage.Flux,
          sourceID: '',
          editMode: QueryEditMode.Advanced,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
      ])
    })
  })

  describe('EDIT_ACTIVE_QUERY_AS_INFLUXQL', () => {
    test('changes the activeQueryEditor and editMode for the currently active query', () => {
      const state = initialStateHelper()

      state.activeQueryIndex = 1
      state.draftQueries = [
        {
          text: 'foo',
          type: InfluxLanguage.InfluxQL,
          sourceID: '',
          editMode: QueryEditMode.Advanced,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
        {
          text: 'bar',
          type: InfluxLanguage.Flux,
          sourceID: '',
          editMode: QueryEditMode.Builder,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
      ]

      const nextState = timeMachineReducer(state, editActiveQueryAsInfluxQL())

      expect(nextState.activeQueryIndex).toEqual(1)
      expect(nextState.draftQueries).toEqual([
        {
          text: 'foo',
          type: InfluxLanguage.InfluxQL,
          sourceID: '',
          editMode: QueryEditMode.Advanced,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
        {
          text: 'bar',
          type: InfluxLanguage.InfluxQL,
          sourceID: '',
          editMode: QueryEditMode.Advanced,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
      ])
    })
  })

  describe('SET_ACTIVE_QUERY_INDEX', () => {
    describe('sets the activeQueryIndex and activeQueryEditor', () => {
      test('shows the builder when active query is in builder mode', () => {
        const state = initialStateHelper()

        state.activeQueryIndex = 1
        state.view.properties.queries = [
          {
            text: 'foo',
            type: InfluxLanguage.Flux,
            sourceID: '',
            editMode: QueryEditMode.Builder,
            builderConfig: {
              buckets: [],
              measurements: [],
              fields: [],
              functions: [],
            },
          },
          {
            text: 'bar',
            type: InfluxLanguage.Flux,
            sourceID: '',
            editMode: QueryEditMode.Advanced,
            builderConfig: {
              buckets: [],
              measurements: [],
              fields: [],
              functions: [],
            },
          },
        ]

        const nextState = timeMachineReducer(state, setActiveQueryIndex(0))

        expect(nextState.activeQueryIndex).toEqual(0)
      })

      test('shows the influxql editor when the active query is influxql and in advanced mode', () => {
        const state = initialStateHelper()

        state.activeQueryIndex = 1
        state.view.properties.queries = [
          {
            text: 'foo',
            type: InfluxLanguage.InfluxQL,
            sourceID: '',
            editMode: QueryEditMode.Advanced,
            builderConfig: {
              buckets: [],
              measurements: [],
              fields: [],
              functions: [],
            },
          },
          {
            text: 'bar',
            type: InfluxLanguage.Flux,
            sourceID: '',
            editMode: QueryEditMode.Builder,
            builderConfig: {
              buckets: [],
              measurements: [],
              fields: [],
              functions: [],
            },
          },
        ]

        const nextState = timeMachineReducer(state, setActiveQueryIndex(0))

        expect(nextState.activeQueryIndex).toEqual(0)
      })

      test('shows the flux editor when the active query is flux and in advanced mode', () => {
        const state = initialStateHelper()

        state.activeQueryIndex = 1
        state.view.properties.queries = [
          {
            text: 'foo',
            type: InfluxLanguage.Flux,
            sourceID: '',
            editMode: QueryEditMode.Advanced,
            builderConfig: {
              buckets: [],
              measurements: [],
              fields: [],
              functions: [],
            },
          },
          {
            text: 'bar',
            type: InfluxLanguage.Flux,
            sourceID: '',
            editMode: QueryEditMode.Builder,
            builderConfig: {
              buckets: [],
              measurements: [],
              fields: [],
              functions: [],
            },
          },
        ]

        const nextState = timeMachineReducer(state, setActiveQueryIndex(0))

        expect(nextState.activeQueryIndex).toEqual(0)
      })
    })
  })

  describe('ADD_QUERY', () => {
    test('adds a query, sets the activeQueryIndex and activeQueryEditor', () => {
      const state = initialStateHelper()

      state.activeQueryIndex = 0
      state.draftQueries = [
        {
          text: 'a',
          type: InfluxLanguage.Flux,
          sourceID: '',
          editMode: QueryEditMode.Advanced,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
      ]

      const nextState = timeMachineReducer(state, addQuery())

      expect(nextState.activeQueryIndex).toEqual(1)
      expect(nextState.draftQueries).toEqual([
        {
          text: 'a',
          type: InfluxLanguage.Flux,
          sourceID: '',
          editMode: QueryEditMode.Advanced,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
        {
          text: '',
          type: InfluxLanguage.Flux,
          sourceID: '',
          editMode: QueryEditMode.Builder,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
      ])
    })
  })

  describe('REMOVE_QUERY', () => {
    let queries: DashboardQuery[]

    beforeEach(() => {
      queries = [
        {
          text: 'a',
          type: InfluxLanguage.Flux,
          sourceID: '',
          editMode: QueryEditMode.Builder,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
        {
          text: 'b',
          type: InfluxLanguage.Flux,
          sourceID: '',
          editMode: QueryEditMode.Builder,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
        {
          text: 'c',
          type: InfluxLanguage.InfluxQL,
          sourceID: '',
          editMode: QueryEditMode.Advanced,
          builderConfig: {
            buckets: [],
            measurements: [],
            fields: [],
            functions: [],
          },
        },
      ]
    })

    test('removes the query and draftScript', () => {
      const state = initialStateHelper()

      state.view.properties.queries = queries
      state.draftQueries = queries
      state.activeQueryIndex = 1

      const nextState = timeMachineReducer(state, removeQuery(1))

      expect(nextState.view.properties.queries).toEqual(queries)
      expect(nextState.draftQueries).toEqual([queries[0], queries[2]])
      expect(nextState.activeQueryIndex).toEqual(1)
    })

    test('sets the activeQueryIndex to the left if was right-most tab', () => {
      const state = initialStateHelper()

      state.view.properties.queries = queries
      state.draftQueries = queries
      state.activeQueryIndex = 2

      const nextState = timeMachineReducer(state, removeQuery(2))

      expect(nextState.view.properties.queries).toEqual(queries)
      expect(nextState.draftQueries).toEqual([queries[0], queries[1]])
      expect(nextState.activeQueryIndex).toEqual(1)
    })
  })

  describe('UPDATE_ACTIVE_QUERY_NAME', () => {
    test('sets the name for the activeQueryIndex', () => {
      const state = initialStateHelper()
      state.activeQueryIndex = 1

      const builderConfig = {
        buckets: [],
        measurements: [],
        fields: [],
        functions: [],
      }

      state.draftQueries = [
        {
          text: 'foo',
          type: InfluxLanguage.Flux,
          sourceID: '',
          editMode: QueryEditMode.Advanced,
          builderConfig,
        },
        {
          text: 'bar',
          type: InfluxLanguage.Flux,
          sourceID: '',
          editMode: QueryEditMode.Builder,
          builderConfig,
        },
      ]

      const nextState = timeMachineReducer(
        state,
        updateActiveQueryName('test query')
      )

      expect(nextState.draftQueries).toEqual([
        {
          text: 'foo',
          type: InfluxLanguage.Flux,
          sourceID: '',
          editMode: QueryEditMode.Advanced,
          builderConfig,
        },
        {
          text: 'bar',
          type: InfluxLanguage.Flux,
          sourceID: '',
          editMode: QueryEditMode.Builder,
          name: 'test query',
          builderConfig,
        },
      ])
    })
  })
})
