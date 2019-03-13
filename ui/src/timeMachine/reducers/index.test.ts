import _ from 'lodash'

// Reducers
import {
  initialState,
  initialStateHelper,
  timeMachineReducer,
  timeMachinesReducer,
} from 'src/timeMachine/reducers'

// Actions
import {
  submitQueries,
  setActiveTab,
  setActiveTimeMachine,
  setActiveQueryIndexSync,
  editActiveQueryWithBuilder,
  editActiveQueryAsFlux,
  addQuerySync,
  removeQuerySync,
  updateActiveQueryName,
  setBackgroundThresholdColoring,
  setTextThresholdColoring,
} from 'src/timeMachine/actions'

// Utils
import {createView} from 'src/shared/utils/view'

// Constants
import {
  DE_TIME_MACHINE_ID,
  VEO_TIME_MACHINE_ID,
} from 'src/timeMachine/constants'

// Types
import {TimeMachineTab} from 'src/types/v2/timeMachine'
import {
  DashboardDraftQuery,
  DashboardQuery,
  QueryViewProperties,
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
        name: '',
        text: 'foo',
        editMode: QueryEditMode.Advanced,
        builderConfig: {buckets: [], tags: [], functions: []},
      },
      {
        name: '',
        text: 'bar',
        editMode: QueryEditMode.Builder,
        builderConfig: {buckets: [], tags: [], functions: []},
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
    expect(
      _.map(nextTimeMachine.draftQueries, q => _.omit(q, ['hidden']))
    ).toEqual(view.properties.queries)
  })
})

describe('timeMachineReducer', () => {
  describe('SUBMIT_QUERIES', () => {
    test('replaces each queries text', () => {
      const state = initialStateHelper()

      const queryA: DashboardDraftQuery = {
        name: '',
        text: 'foo',
        editMode: QueryEditMode.Builder,
        builderConfig: {buckets: [], tags: [], functions: []},
        hidden: false,
      }

      const queryB: DashboardQuery = {
        name: '',
        text: 'bar',
        editMode: QueryEditMode.Builder,
        builderConfig: {buckets: [], tags: [], functions: []},
      }

      state.view.properties.queries = [queryA, queryB]
      state.draftQueries = [
        {...queryA, text: 'baz', hidden: false},
        {...queryB, text: 'buzz', hidden: false},
      ]

      const actual = timeMachineReducer(state, submitQueries()).view.properties
        .queries

      const expected = state.draftQueries

      expect(actual).toEqual(expected)
    })
  })

  describe('EDIT_ACTIVE_QUERY_WITH_BUILDER', () => {
    test('changes the activeQueryEditor and editMode for the currently active query', () => {
      const state = initialStateHelper()

      state.activeQueryIndex = 1
      state.draftQueries = [
        {
          name: '',
          text: 'foo',
          editMode: QueryEditMode.Builder,
          builderConfig: {buckets: [], tags: [], functions: []},
          hidden: false,
        },
        {
          name: '',
          text: 'bar',
          editMode: QueryEditMode.Advanced,
          builderConfig: {buckets: [], tags: [], functions: []},
          hidden: false,
        },
      ]

      const nextState = timeMachineReducer(state, editActiveQueryWithBuilder())

      expect(nextState.activeQueryIndex).toEqual(1)
      expect(nextState.draftQueries).toEqual([
        {
          name: '',
          text: '',
          editMode: QueryEditMode.Builder,
          builderConfig: {buckets: [], tags: [], functions: []},
          hidden: false,
        },
        {
          name: '',
          text: '',
          editMode: QueryEditMode.Builder,
          builderConfig: {buckets: [], tags: [], functions: []},
          hidden: false,
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
          name: '',
          text: 'foo',
          editMode: QueryEditMode.Advanced,
          builderConfig: {buckets: [], tags: [], functions: []},
          hidden: false,
        },
        {
          name: '',
          text: 'bar',
          editMode: QueryEditMode.Builder,
          builderConfig: {buckets: [], tags: [], functions: []},
          hidden: false,
        },
      ]

      const nextState = timeMachineReducer(state, editActiveQueryAsFlux())

      expect(nextState.activeQueryIndex).toEqual(1)
      expect(nextState.draftQueries).toEqual([
        {
          name: '',
          text: 'foo',
          editMode: QueryEditMode.Advanced,
          builderConfig: {buckets: [], tags: [], functions: []},
          hidden: false,
        },
        {
          name: '',
          text: 'bar',
          editMode: QueryEditMode.Advanced,
          builderConfig: {buckets: [], tags: [], functions: []},
          hidden: false,
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
            name: '',
            text: 'foo',
            editMode: QueryEditMode.Builder,
            builderConfig: {buckets: [], tags: [], functions: []},
          },
          {
            name: '',
            text: 'bar',
            editMode: QueryEditMode.Advanced,
            builderConfig: {buckets: [], tags: [], functions: []},
          },
        ]

        const nextState = timeMachineReducer(state, setActiveQueryIndexSync(0))

        expect(nextState.activeQueryIndex).toEqual(0)
      })

      test('shows the influxql editor when the active query is influxql and in advanced mode', () => {
        const state = initialStateHelper()

        state.activeQueryIndex = 1
        state.view.properties.queries = [
          {
            name: '',
            text: 'foo',
            editMode: QueryEditMode.Advanced,
            builderConfig: {buckets: [], tags: [], functions: []},
          },
          {
            name: '',
            text: 'bar',
            editMode: QueryEditMode.Builder,
            builderConfig: {buckets: [], tags: [], functions: []},
          },
        ]

        const nextState = timeMachineReducer(state, setActiveQueryIndexSync(0))

        expect(nextState.activeQueryIndex).toEqual(0)
      })

      test('shows the flux editor when the active query is flux and in advanced mode', () => {
        const state = initialStateHelper()

        state.activeQueryIndex = 1
        state.view.properties.queries = [
          {
            name: '',
            text: 'foo',
            editMode: QueryEditMode.Advanced,
            builderConfig: {buckets: [], tags: [], functions: []},
          },
          {
            name: '',
            text: 'bar',
            editMode: QueryEditMode.Builder,
            builderConfig: {buckets: [], tags: [], functions: []},
          },
        ]

        const nextState = timeMachineReducer(state, setActiveQueryIndexSync(0))

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
          name: '',
          text: 'a',
          editMode: QueryEditMode.Advanced,
          builderConfig: {buckets: [], tags: [], functions: []},
          hidden: false,
        },
      ]

      const nextState = timeMachineReducer(state, addQuerySync())

      expect(nextState.activeQueryIndex).toEqual(1)
      expect(nextState.draftQueries).toEqual([
        {
          name: '',
          text: 'a',
          editMode: QueryEditMode.Advanced,
          builderConfig: {buckets: [], tags: [], functions: []},
          hidden: false,
        },
        {
          name: '',
          text: '',
          editMode: QueryEditMode.Builder,
          builderConfig: {
            buckets: [],
            tags: [{key: '_measurement', values: []}],
            functions: [],
          },
          hidden: false,
        },
      ])
    })
  })

  describe('REMOVE_QUERY', () => {
    let queries: DashboardDraftQuery[]

    beforeEach(() => {
      queries = [
        {
          name: '',
          text: 'a',
          editMode: QueryEditMode.Builder,
          builderConfig: {buckets: [], tags: [], functions: []},
          hidden: false,
        },
        {
          name: '',
          text: 'b',
          editMode: QueryEditMode.Builder,
          builderConfig: {buckets: [], tags: [], functions: []},
          hidden: false,
        },
        {
          name: '',
          text: 'c',
          editMode: QueryEditMode.Advanced,
          builderConfig: {buckets: [], tags: [], functions: []},
          hidden: false,
        },
      ]
    })

    test('removes the query and draftScript', () => {
      const state = initialStateHelper()

      state.view.properties.queries = queries
      state.draftQueries = queries
      state.activeQueryIndex = 1

      const nextState = timeMachineReducer(state, removeQuerySync(1))

      expect(nextState.draftQueries).toEqual([queries[0], queries[2]])
      expect(nextState.activeQueryIndex).toEqual(1)
    })

    test('sets the activeQueryIndex to the left if was right-most tab', () => {
      const state = initialStateHelper()

      state.view.properties.queries = queries
      state.draftQueries = queries
      state.activeQueryIndex = 2

      const nextState = timeMachineReducer(state, removeQuerySync(2))

      expect(nextState.draftQueries).toEqual([queries[0], queries[1]])
      expect(nextState.activeQueryIndex).toEqual(1)
    })
  })

  describe('UPDATE_ACTIVE_QUERY_NAME', () => {
    test('sets the name for the activeQueryIndex', () => {
      const state = initialStateHelper()
      state.activeQueryIndex = 1

      const builderConfig = {buckets: [], tags: [], functions: []}

      state.draftQueries = [
        {
          name: '',
          text: 'foo',
          editMode: QueryEditMode.Advanced,
          builderConfig,
          hidden: false,
        },
        {
          name: '',
          text: 'bar',
          editMode: QueryEditMode.Builder,
          builderConfig,
          hidden: false,
        },
      ]

      const nextState = timeMachineReducer(
        state,
        updateActiveQueryName('test query')
      )

      expect(nextState.draftQueries).toEqual([
        {
          name: '',
          text: 'foo',
          editMode: QueryEditMode.Advanced,
          builderConfig,
          hidden: false,
        },
        {
          text: 'bar',
          editMode: QueryEditMode.Builder,
          name: 'test query',
          builderConfig,
          hidden: false,
        },
      ])
    })
  })

  describe('SET_TEXT_THRESHOLD_COLORING', () => {
    test('sets all color types to text', () => {
      const state = initialStateHelper()

      state.view.properties.colors = [
        {
          hex: '#BF3D5E',
          id: 'base',
          name: 'ruby',
          type: 'background',
          value: 0,
        },
        {
          hex: '#F48D38',
          id: '72bad47c-cec3-4523-8f13-1fabd192ef92',
          name: 'tiger',
          type: 'background',
          value: 22.72,
        },
      ]

      const actual = timeMachineReducer(state, setTextThresholdColoring()).view
        .properties.colors

      const expected = [
        {
          hex: '#BF3D5E',
          id: 'base',
          name: 'ruby',
          type: 'text',
          value: 0,
        },
        {
          hex: '#F48D38',
          id: '72bad47c-cec3-4523-8f13-1fabd192ef92',
          name: 'tiger',
          type: 'text',
          value: 22.72,
        },
      ]

      expect(actual).toEqual(expected)
    })
  })

  describe('SET_BACKGROUND_THRESHOLD_COLORING', () => {
    test('sets all color types to background', () => {
      const state = initialStateHelper()

      state.view.properties.colors = [
        {
          hex: '#BF3D5E',
          id: 'base',
          name: 'ruby',
          type: 'text',
          value: 0,
        },
        {
          hex: '#F48D38',
          id: '72bad47c-cec3-4523-8f13-1fabd192ef92',
          name: 'tiger',
          type: 'text',
          value: 22.72,
        },
      ]

      const actual = timeMachineReducer(state, setBackgroundThresholdColoring())
        .view.properties.colors

      const expected = [
        {
          hex: '#BF3D5E',
          id: 'base',
          name: 'ruby',
          type: 'background',
          value: 0,
        },
        {
          hex: '#F48D38',
          id: '72bad47c-cec3-4523-8f13-1fabd192ef92',
          name: 'tiger',
          type: 'background',
          value: 22.72,
        },
      ]

      expect(actual).toEqual(expected)
    })
  })
})
