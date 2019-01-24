import React from 'react'
import {Provider} from 'react-redux'
import {render} from 'react-testing-library'

import TimeMachineQueryBuilder from 'src/shared/components/TimeMachineQueryBuilder'
import configureStore from 'src/store/configureStore'
import {createMemoryHistory} from 'history'

jest.mock('src/shared/actions/v2/queryBuilder')

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

function renderWithRedux(ui) {
  const store = configureStore(localState, history)

  return {
    ...render(<Provider store={store}>{ui}</Provider>),
    store,
  }
}

describe('TimeMachineQueryBuilder', () => {
  it('can render with redux with defaults', () => {
    renderWithRedux(<TimeMachineQueryBuilder />)
  })
})
