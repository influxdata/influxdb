// Libraries
import React from 'react'
import {render} from '@testing-library/react'

// Components
import WriteDataHelperTokens from 'src/writeData/components/WriteDataHelperTokens'

// Mocks
import {auth} from 'mocks/dummyData'

// Types
import {
  WriteDataDetailsContext,
  DEFAULT_WRITE_DATA_DETAILS_CONTEXT,
} from 'src/writeData/components/WriteDataDetailsContext'

// NOTE: stubbing is required here as the CopyButton component
// requires a redux store (alex)
jest.mock('src/shared/components/CopyButton', () => () => null)

const setup = (override?) => {
  const mockContextValue = {
    ...DEFAULT_WRITE_DATA_DETAILS_CONTEXT,
    tokens: [auth],
    token: auth,
    change: () => {},
  }

  const contextValue = {...mockContextValue, ...override}

  const wrapper = render(
    <WriteDataDetailsContext.Provider value={contextValue}>
      <WriteDataHelperTokens />
    </WriteDataDetailsContext.Provider>
  )

  return {wrapper}
}

describe('WriteDataHelperTokens', () => {
  beforeEach(() => {
    jest.resetModules()
  })

  describe('renders with Tokens', () => {
    it('displays a list and the first Token is selected', () => {
      const {wrapper} = setup()
      const {getByTestId} = wrapper

      const list = getByTestId('write-data-tokens-list')

      expect(list).toBeTruthy()

      const token = getByTestId(auth.description)

      expect(token).toBeTruthy()

      expect(token.classList.contains('cf-list-item__active')).toBeTruthy()
    })
  })

  describe('renders with Tokens and without a selected Token', () => {
    it('displays tokens list without any items selected', () => {
      const {wrapper} = setup({
        token: null,
      })
      const {getByTestId} = wrapper

      const list = getByTestId('write-data-tokens-list')

      expect(list).toBeTruthy()

      const token = getByTestId(auth.description)

      expect(token).toBeTruthy()

      expect(token.classList.contains('cf-list-item__active')).toBeFalsy()
    })
  })

  describe('renders without Tokens', () => {
    it('displays a friendly empty state', () => {
      const {wrapper} = setup({tokens: []})
      const {getByTestId} = wrapper

      const emptyText = getByTestId('write-data-tokens-list-empty')

      expect(emptyText).toBeTruthy()
    })
  })
})
