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
  setActiveTab,
  setActiveTimeMachine,
  setActiveQueryIndexSync,
  editActiveQueryWithBuilderSync,
  editActiveQueryAsFlux,
  addQuerySync,
  removeQuerySync,
  updateActiveQueryName,
  setBackgroundThresholdColoring,
  setTextThresholdColoring,
} from 'src/timeMachine/actions'

// Utils
import {createView} from 'src/shared/utils/view'

// Types
import {DashboardDraftQuery, QueryViewProperties} from 'src/types/dashboards'
import {selectAggregateWindow} from '../actions/queryBuilder'

describe('timeMachinesReducer', () => {
  test('it directs actions to the currently active timeMachine', () => {
    const state = initialState()
    const de = state.timeMachines['de']
    const veo = state.timeMachines['veo']

    expect(state.activeTimeMachineID).toEqual('de')
    expect(de.activeTab).toEqual('queries')
    expect(veo.activeTab).toEqual('queries')

    const nextState = timeMachinesReducer(state, setActiveTab('visualization'))

    const nextDE = nextState.timeMachines['de']
    const nextVEO = nextState.timeMachines['veo']

    expect(nextDE.activeTab).toEqual('visualization')
    expect(nextVEO.activeTab).toEqual('queries')
  })

  test('it resets tab and draftScript state on a timeMachine when activated', () => {
    const state = initialState()

    expect(state.activeTimeMachineID).toEqual('de')

    const activeTimeMachine = state.timeMachines[state.activeTimeMachineID]

    activeTimeMachine.activeQueryIndex = 2

    const view = createView<QueryViewProperties>()

    view.properties.queries = [
      {
        name: '',
        text: 'foo',
        editMode: 'advanced',
        builderConfig: {
          buckets: [],
          tags: [],
          functions: [],
          aggregateWindow: {period: 'auto'},
        },
      },
      {
        name: '',
        text: 'bar',
        editMode: 'builder',
        builderConfig: {
          buckets: [],
          tags: [],
          functions: [],
          aggregateWindow: {period: 'auto'},
        },
      },
    ]

    const nextState = timeMachinesReducer(
      state,
      setActiveTimeMachine('veo', {view})
    )

    expect(nextState.activeTimeMachineID).toEqual('veo')

    const nextTimeMachine =
      nextState.timeMachines[nextState.activeTimeMachineID]

    expect(nextTimeMachine.activeTab).toEqual('queries')
    expect(nextTimeMachine.activeQueryIndex).toEqual(0)
    expect(
      _.map(nextTimeMachine.draftQueries, q => _.omit(q, ['hidden']))
    ).toEqual(view.properties.queries)
  })
})

describe('timeMachineReducer', () => {
  describe('EDIT_ACTIVE_QUERY_WITH_BUILDER', () => {
    test('changes the activeQueryEditor and editMode for the currently active query', () => {
      const state = initialStateHelper()

      state.activeQueryIndex = 1
      state.draftQueries = [
        {
          name: '',
          text: 'foo',
          editMode: 'builder',
          builderConfig: {
            buckets: [],
            tags: [],
            functions: [],
            aggregateWindow: {period: 'auto'},
          },
          hidden: false,
        },
        {
          name: '',
          text: 'bar',
          editMode: 'advanced',
          builderConfig: {
            buckets: [],
            tags: [],
            functions: [],
            aggregateWindow: {period: 'auto'},
          },
          hidden: false,
        },
      ]

      const nextState = timeMachineReducer(
        state,
        editActiveQueryWithBuilderSync()
      )

      expect(nextState.activeQueryIndex).toEqual(1)
      expect(nextState.draftQueries).toEqual([
        {
          name: '',
          text: '',
          editMode: 'builder',
          builderConfig: {
            buckets: [],
            tags: [],
            functions: [],
            aggregateWindow: {period: 'auto'},
          },
          hidden: false,
        },
        {
          name: '',
          text: '',
          editMode: 'builder',
          builderConfig: {
            buckets: [],
            tags: [],
            functions: [],
            aggregateWindow: {period: 'auto'},
          },
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
          editMode: 'advanced',
          builderConfig: {
            buckets: [],
            tags: [],
            functions: [],
            aggregateWindow: {period: 'auto'},
          },
          hidden: false,
        },
        {
          name: '',
          text: 'bar',
          editMode: 'builder',
          builderConfig: {
            buckets: [],
            tags: [],
            functions: [],
            aggregateWindow: {period: 'auto'},
          },
          hidden: false,
        },
      ]

      const nextState = timeMachineReducer(state, editActiveQueryAsFlux())

      expect(nextState.activeQueryIndex).toEqual(1)
      expect(nextState.draftQueries).toEqual([
        {
          name: '',
          text: 'foo',
          editMode: 'advanced',
          builderConfig: {
            buckets: [],
            tags: [],
            functions: [],
            aggregateWindow: {period: 'auto'},
          },
          hidden: false,
        },
        {
          name: '',
          text: 'bar',
          editMode: 'advanced',
          builderConfig: {
            buckets: [],
            tags: [],
            functions: [],
            aggregateWindow: {period: 'auto'},
          },
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
            editMode: 'builder',
            builderConfig: {
              buckets: [],
              tags: [],
              functions: [],
              aggregateWindow: {period: 'auto'},
            },
          },
          {
            name: '',
            text: 'bar',
            editMode: 'advanced',
            builderConfig: {
              buckets: [],
              tags: [],
              functions: [],
              aggregateWindow: {period: 'auto'},
            },
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
            editMode: 'advanced',
            builderConfig: {
              buckets: [],
              tags: [],
              functions: [],
              aggregateWindow: {period: 'auto'},
            },
          },
          {
            name: '',
            text: 'bar',
            editMode: 'builder',
            builderConfig: {
              buckets: [],
              tags: [],
              functions: [],
              aggregateWindow: {period: 'auto'},
            },
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
            editMode: 'advanced',
            builderConfig: {
              buckets: [],
              tags: [],
              functions: [],
              aggregateWindow: {period: 'auto'},
            },
          },
          {
            name: '',
            text: 'bar',
            editMode: 'builder',
            builderConfig: {
              buckets: [],
              tags: [],
              functions: [],
              aggregateWindow: {period: 'auto'},
            },
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
          editMode: 'advanced',
          builderConfig: {
            buckets: [],
            tags: [],
            functions: [],
            aggregateWindow: {period: 'auto'},
          },
          hidden: false,
        },
      ]

      const nextState = timeMachineReducer(state, addQuerySync())

      expect(nextState.activeQueryIndex).toEqual(1)
      expect(nextState.draftQueries).toEqual([
        {
          name: '',
          text: 'a',
          editMode: 'advanced',
          builderConfig: {
            buckets: [],
            tags: [],
            functions: [],
            aggregateWindow: {period: 'auto'},
          },
          hidden: false,
        },
        {
          name: '',
          text: '',
          editMode: 'builder',
          builderConfig: {
            buckets: [],
            tags: [{key: '_measurement', values: []}],
            functions: [],
            aggregateWindow: {period: 'auto'},
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
          editMode: 'builder',
          builderConfig: {
            buckets: [],
            tags: [],
            functions: [],
            aggregateWindow: {period: 'auto'},
          },
          hidden: false,
        },
        {
          name: '',
          text: 'b',
          editMode: 'builder',
          builderConfig: {
            buckets: [],
            tags: [],
            functions: [],
            aggregateWindow: {period: 'auto'},
          },
          hidden: false,
        },
        {
          name: '',
          text: 'c',
          editMode: 'advanced',
          builderConfig: {
            buckets: [],
            tags: [],
            functions: [],
            aggregateWindow: {period: 'auto'},
          },
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

      const builderConfig = {
        buckets: [],
        tags: [],
        functions: [],
        aggregateWindow: {period: 'auto'},
      }

      state.draftQueries = [
        {
          name: '',
          text: 'foo',
          editMode: 'advanced',
          builderConfig,
          hidden: false,
        },
        {
          name: '',
          text: 'bar',
          editMode: 'builder',
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
          editMode: 'advanced',
          builderConfig,
          hidden: false,
        },
        {
          text: 'bar',
          editMode: 'builder',
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

  describe('SET_AGGREGATE_WINDOW', () => {
    const state = initialStateHelper()
    state.activeQueryIndex = 1

    const builderConfig = {
      buckets: [],
      tags: [],
      functions: [],
      aggregateWindow: {period: 'auto'},
    }

    const dq0 = {
      name: '',
      text: '',
      editMode: 'advanced' as 'advanced',
      builderConfig,
      hidden: false,
    }

    const dq1 = {
      name: '',
      text: '',
      editMode: 'builder' as 'builder',
      builderConfig,
      hidden: false,
    }

    state.draftQueries = [dq0, dq1]

    const period = '15m'
    const nextState = timeMachineReducer(state, selectAggregateWindow(period))
    const updatedConfig = {
      ...builderConfig,
      aggregateWindow: {period},
    }

    expect(nextState.draftQueries).toEqual([
      dq0,
      {...dq1, builderConfig: updatedConfig},
    ])
  })
})
