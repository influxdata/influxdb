// Libraries
import {mocked} from 'ts-jest/utils'
import fetchMock from 'jest-fetch-mock'

// Utils
import {hydrateVariables} from 'src/variables/actions/thunks'
import {mockAppState} from 'src/mockAppState'

describe('hydrateVariables', () => {
  it('expect this to pass', () => {
    expect(true).toBe(true)
  })
  it('should only call the hydrateVars once per query', () => {
    const dispatch = jest.fn()
    const getState = jest.fn(() => mockAppState)

    let count = 0
    fetchMock.mockResponse(
      req =>
        new Promise(() => {
          count++
          console.log(req)
          return {foo: 'bar'}
        })
    )
    hydrateVariables()(dispatch, getState)
    expect(count).toBe(1)
  })
})
