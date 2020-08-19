// Libraries
import React from 'react'
import {Provider} from 'react-redux'
import {Router} from 'react-router-dom'
import {createMemoryHistory} from 'history'
import {render} from '@testing-library/react'

// Reducers
import {initialState as initialVariablesState} from 'src/variables/reducers'
import {initialState as initialUserSettingsState} from 'src/userSettings/reducers'
import {initialStateHelper as timeMachineState} from 'src/timeMachine/reducers'
import {default as configureStore, clearStore} from 'src/store/configureStore'
import {pastFifteenMinTimeRange} from './shared/constants/timeRanges'

// Types
import {RemoteDataState, TimeZone, LocalStorage, ResourceType} from 'src/types'

const {Orgs} = ResourceType
const {Done} = RemoteDataState

export const localState: LocalStorage = {
  app: {
    ephemeral: {
      inPresentationMode: false,
      hasUpdatedTimeRangeInVEO: false,
    },
    persisted: {
      autoRefresh: 0,
      showTemplateControlBar: false,
      navBarState: 'expanded',
      timeZone: 'Local' as TimeZone,
      theme: 'dark',
      notebookMiniMapState: 'expanded',
    },
  },
  flags: {
    status: RemoteDataState.Done,
    original: {},
    override: {},
  },
  VERSION: '2.0.0',
  ranges: {
    '0349ecda531ea000': pastFifteenMinTimeRange,
  },
  autoRefresh: {},
  userSettings: initialUserSettingsState(),
  resources: {
    [Orgs]: {
      byID: {
        orgid: {
          name: 'org',
          id: 'orgid',
        },
      },
      allIDs: ['orgid'],
      org: {name: 'org', id: 'orgid'},
      status: Done,
    },
    variables: initialVariablesState(),
  },
  timeMachines: {
    activeTimeMachineID: 'de',
    timeMachines: {
      de: timeMachineState(),
    },
  },
}

export function renderWithRedux(ui, initialState = s => s) {
  clearStore()
  const seedStore = configureStore(localState)
  const seedState = seedStore.getState()
  clearStore()
  const store = configureStore(initialState(seedState))

  return {
    ...render(<Provider store={store}>{ui}</Provider>),
    store,
  }
}

export function renderWithReduxAndRouter(ui, initialState = s => s) {
  clearStore()
  const history = createMemoryHistory({initialEntries: ['/']})
  const seedStore = configureStore(localState)
  const seedState = seedStore.getState()
  clearStore()
  const store = configureStore(initialState(seedState))

  return {
    ...render(
      <Provider store={store}>
        <Router history={history}>{ui}</Router>
      </Provider>
    ),
    store,
  }
}

export function renderWithRouter(
  ui,
  {route = '/', history = createMemoryHistory({initialEntries: [route]})} = {}
) {
  return {
    ...render(<Router history={history}>{ui}</Router>),
    history,
  }
}
