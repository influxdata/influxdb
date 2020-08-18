// Libraries
import React from 'react'
import {renderWithReduxAndRouter} from 'src/mockState'

// Components
import WriteDataDetailsView from 'src/writeData/components/WriteDataDetailsView'

// NOTE: stubbing is required here as the CopyButton component
// requires a redux store (alex)
jest.mock('src/shared/components/CopyButton', () => () => null)

describe('WriteDataDetailsView', () => {
  beforeEach(() => {
    jest.resetModules()
  })

  describe('rendering with tokens', () => {
    it('renders', () => {
      const {getByTestId} = renderWithReduxAndRouter(<WriteDataDetailsView />)

      const root = getByTestId('variable-form--root')

      expect(root).toBeTruthy()
    })
  })
})
