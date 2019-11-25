// Libraries
import React from 'react'
import {render} from 'react-testing-library'

describe('Telegrafs.Components.TelegrafOutputOverlay', () => {
  beforeEach(() => {
    jest.resetModules()
  })

  it('should render when there are no buckets', () => {
    const props = {
      org: 'neateo',
      orgID: '1234',
      server: 'localhost',
      buckets: [],
      onClose: () => {},
    }
    // NOTE: stubbing is required here as the CopyButton component
    // requires a redux store (alex)
    jest.mock('src/shared/components/CopyButton', () => () => null)
    const {
      TelegrafOutputOverlay,
    } = require('src/telegrafs/components/TelegrafOutputOverlay')
    const {getByTestId} = render(<TelegrafOutputOverlay {...props} />)

    const root = getByTestId('telegraf-output-overlay--code-snippet')

    expect(root).toBeTruthy()
  })
})
