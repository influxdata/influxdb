import React from 'react'

import {renderWithRedux} from 'src/mockState'
import {source} from 'mocks/dummyData'
import {waitForElement, fireEvent} from 'react-testing-library'

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

  it.only('can select a bucket', async () => {
    const {getByTestId, getAllByTestId, queryAllByTestId} = renderWithRedux(
      <TimeMachineQueryBuilder />,
      setInitialState
    )

    let bucketsDropdown = await waitForElement(() =>
      getByTestId('buckets--button')
    )

    fireEvent.click(bucketsDropdown)

    const bucketsMenu = await waitForElement(() =>
      getAllByTestId('dropdown--item')
    )

    expect(bucketsMenu.length).toBe(2)

    const b2 = getByTestId('dropdown--item b2')

    fireEvent.click(b2)

    bucketsDropdown = await waitForElement(() => getByTestId('buckets--button'))

    expect(bucketsDropdown.textContent).toBe('b2')
    expect(queryAllByTestId('dropdown--item')).toBe(null)
  })

  it('can select a measurement', async () => {
    const {
      getByText,
      getByTestId,
      getAllByTestId,
      queryAllByTestId,
    } = renderWithRedux(<TimeMachineQueryBuilder />, setInitialState)

    let keysButton = await waitForElement(() => getByText('tk1'))

    fireEvent.click(keysButton)

    const keysMenuItems = await waitForElement(() =>
      getAllByTestId('dropdown--item')
    )

    expect(keysMenuItems.length).toBe(2)

    const tk2 = getByTestId('dropdown--item tk2')

    fireEvent.click(tk2)

    keysButton = await waitForElement(() =>
      getByTestId('tag-selector--dropdown')
    )

    expect(keysButton.innerText).toBe('tk2')
    expect(queryAllByTestId('dropdown--item')).toBe(null)
  })
})
