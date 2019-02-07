import React from 'react'
import {Provider} from 'react-redux'

import {render} from 'react-testing-library'
import configureStore from 'src/store/configureStore'
import {createMemoryHistory} from 'history'

const localState = {
  app: {
    ephemeral: {
      inPresentationMode: false,
    },
    persisted: {autoRefresh: 0, showTemplateControlBar: false},
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
}

const history = createMemoryHistory({entries: ['/']})

export function renderWithRedux(ui, initialState = s => s) {
  const seedStore = configureStore(localState, history)
  const seedState = seedStore.getState()
  const store = configureStore(initialState(seedState), history)

  const provider = <Provider store={store}>{ui}</Provider>

  return {
    ...render(provider),
    store,
  }
}
