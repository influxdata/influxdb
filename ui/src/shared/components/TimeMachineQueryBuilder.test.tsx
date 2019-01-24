import React from 'react'

import {renderWithRedux} from 'src/mockState'
import {source} from 'mocks/dummyData'

import TimeMachineQueryBuilder from 'src/shared/components/TimeMachineQueryBuilder'

jest.mock('src/shared/apis/v2/queryBuilder')

let setInitialState
describe('TimeMachineQueryBuilder', () => {
  beforeEach(() => {
    setInitialState = state => {
      return {
        ...state,
        sources: {
          activeSourceID: source.id,
          sources: {
            [source.id]: source,
          },
        },
      }
    }
  })

  it('can render with redux with defaults', () => {
    const {getByTestId} = renderWithRedux(
      <TimeMachineQueryBuilder />,
      setInitialState
    )

    const builder = getByTestId('query-builder')
    expect(builder).toMatchSnapshot()

    // const buckets = getByTestId('buckets-dropdown')
  })
})
