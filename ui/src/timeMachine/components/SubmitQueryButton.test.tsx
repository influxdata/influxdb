// Libraries
import React from 'react'
import {mocked} from 'ts-jest/utils'
import {fireEvent} from '@testing-library/react'

import {renderWithRedux} from 'src/mockState'
import {RemoteDataState} from 'src/types'

declare global {
  interface Window {
    TextDecoder: any
  }
}

class FakeTextDecoder {
  decode() {
    return ''
  }
}

window.TextDecoder = FakeTextDecoder

jest.mock('src/external/parser', () => {
  return {
    parse: jest.fn(() => {
      return {
        type: 'File',
        package: {
          name: {
            name: 'fake',
            type: 'Identifier',
          },
          type: 'PackageClause',
        },
        imports: [],
        body: [],
      }
    }),
  }
})

jest.mock('src/variables/actions/thunks', () => {
  return {
    hydrateVariables: jest.fn(() => {
      return (_dispatch, _getState) => {
        return Promise.resolve()
      }
    }),
  }
})

import SubmitQueryButton from 'src/timeMachine/components/SubmitQueryButton'

const stateOverride = {
  timeMachines: {
    activeTimeMachineID: 'veo',
    timeMachines: {
      veo: {
        draftQueries: [
          {
            text: `from(bucket: "apps")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "rum")
  |> filter(fn: (r) => r["_field"] == "domInteractive")
  |> map(fn: (r) => ({r with _value: r._value / 1000.0}))
  |> group()`,
          },
        ],
        activeQueryIndex: 0,
        queryResults: {
          status: RemoteDataState.NotStarted,
        },
        view: {
          properties: {
            queries: [{text: 'draftstate'}],
          },
        },
      },
    },
  },
}

describe('TimeMachine.Components.SubmitQueryButton', () => {
  beforeEach(() => {
    jest.useFakeTimers()
  })
  afterEach(() => {
    jest.useRealTimers()
  })

  it('it changes the Submit button to Cancel when the request is in flight, then back to Submit after the request has resolved', async () => {
    const fakeReader = {
      cancel: jest.fn(),
      read: jest.fn(() => {
        return Promise.resolve({
          done: true,
        })
      }),
    }

    const fakeResponse = {
      status: 200,
      body: {
        getReader: () => fakeReader,
      },
    }

    const expectedMockedFetchCall = {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'Accept-Encoding': 'gzip'},
      body: JSON.stringify({
        query: stateOverride.timeMachines.timeMachines.veo.draftQueries[0].text,
        extern: {
          type: 'File',
          package: null,
          imports: null,
          body: [
            {
              type: 'OptionStatement',
              assignment: {
                type: 'VariableAssignment',
                id: {type: 'Identifier', name: 'v'},
                init: {
                  type: 'ObjectExpression',
                  properties: [
                    {
                      type: 'Property',
                      key: {type: 'Identifier', name: 'timeRangeStart'},
                      value: {
                        type: 'UnaryExpression',
                        operator: '-',
                        argument: {
                          type: 'DurationLiteral',
                          values: [{magnitude: 1, unit: 'h'}],
                        },
                      },
                    },
                    {
                      type: 'Property',
                      key: {type: 'Identifier', name: 'timeRangeStop'},
                      value: {
                        type: 'CallExpression',
                        callee: {type: 'Identifier', name: 'now'},
                      },
                    },
                  ],
                },
              },
            },
          ],
        },
        dialect: {annotations: ['group', 'datatype', 'default']},
      }),
      signal: new AbortController().signal,
    }

    mocked(fetch).mockImplementation(() => {
      return Promise.resolve(fakeResponse)
    })
    const {getByTitle} = renderWithRedux(<SubmitQueryButton />, s => ({
      ...s,
      ...stateOverride,
    }))

    fireEvent.click(getByTitle('Submit'))
    expect(getByTitle('Submit')).toBeTruthy()
    await window.flushAllPromises()

    expect(mocked(fetch)).toHaveBeenCalledWith(
      'http://example.com/api/v2/query?orgID=orgid',
      expectedMockedFetchCall
    )
    expect(getByTitle('Submit')).toBeTruthy()
  })

  it("cancels the query after submission if the query hasn't finished and resolved", async () => {
    mocked(fetchMock).mockResponse(() => {
      return new Promise((resolve, _reject) => {
        setTimeout(() => {
          resolve('')
        }, 4000)
      })
    })

    const {getByTitle} = renderWithRedux(<SubmitQueryButton />, s => ({
      ...s,
      ...stateOverride,
    }))
    const SubmitBtn = getByTitle('Submit')
    fireEvent.click(SubmitBtn)

    const CancelBtn = getByTitle('Cancel')
    fireEvent.click(CancelBtn)
    await window.flushAllPromises()

    const {type, value: error} = mocked(fetch).mock.results[0] as any
    expect(type).toBe('throw')
    expect(error.name).toBe('AbortError')

    expect(getByTitle('Submit')).toBeTruthy()
  })
})
