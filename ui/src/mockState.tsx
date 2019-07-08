import React from 'react'
import {Provider} from 'react-redux'
import {Router, createMemoryHistory} from 'react-router'

import {render} from 'react-testing-library'
import {initialState as initialVariablesState} from 'src/variables/reducers'
import {initialState as initialUserSettingsState} from 'src/userSettings/reducers'
import configureStore from 'src/store/configureStore'
import {RemoteDataState} from 'src/types'

const localState = {
  app: {
    ephemeral: {
      inPresentationMode: false,
    },
    persisted: {autoRefresh: 0, showTemplateControlBar: false},
  },
  orgs: {
    items: [{name: 'org', orgID: 'orgid'}],
    org: {name: 'org', id: 'orgid'},
    status: RemoteDataState.Done,
  },
  VERSION: '2.0.0',
  ranges: [
    {
      dashboardID: '0349ecda531ea000',
      seconds: 900,
      lower: 'now() - 15m',
      upper: null,
      label: 'Past 15m',
      duration: '15m',
    },
  ],
  autoRefresh: {},
  variables: initialVariablesState(),
  userSettings: initialUserSettingsState(),
}

const history = createMemoryHistory({entries: ['/']})

export function renderWithRedux(ui, initialState = s => s) {
  const seedStore = configureStore(localState, history)
  const seedState = seedStore.getState()
  const store = configureStore(initialState(seedState), history)

  return {
    ...render(<Provider store={store}>{ui}</Provider>),
    store,
  }
}

export function renderWithReduxAndRouter(
  ui,
  initialState = s => s,
  {route = '/', history = createMemoryHistory({entries: [route]})} = {}
) {
  const seedStore = configureStore(localState, history)
  const seedState = seedStore.getState()
  const store = configureStore(initialState(seedState), history)

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
  {route = '/', history = createMemoryHistory({entries: [route]})} = {}
) {
  return {
    ...render(<Router history={history}>{ui}</Router>),
    history,
  }
}
