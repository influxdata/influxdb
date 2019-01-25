import React from 'react'

import {renderWithRedux} from 'src/mockState'
import {source} from 'mocks/dummyData'
import {waitForElement, fireEvent} from 'react-testing-library'

import QueryBuilder from 'src/timeMachine/components/QueryBuilder'

jest.mock('src/timeMachine/apis/queryBuilder')

let setInitialState
describe('QueryBuilder', () => {
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

  it('can select a bucket', async () => {
    const {getByTestId, getAllByTestId, queryAllByTestId} = renderWithRedux(
      <QueryBuilder />,
      setInitialState
    )

    let bucketsDropdownClosed = await waitForElement(() =>
      getByTestId('buckets--button')
    )

    fireEvent.click(bucketsDropdownClosed)

    const bucketItems = await waitForElement(() =>
      getAllByTestId(/dropdown--item/)
    )

    expect(bucketItems.length).toBe(2)

    const b2 = getByTestId('dropdown--item b2')

    fireEvent.click(b2)

    bucketsDropdownClosed = await waitForElement(() =>
      getByTestId('buckets--button')
    )

    expect(bucketsDropdownClosed.textContent).toBe('b2')
    expect(queryAllByTestId(/dropdown--item/).length).toBe(0)
  })

  it('can select a tag', async () => {
    const {
      getByText,
      getByTestId,
      getAllByTestId,
      queryAllByTestId,
    } = renderWithRedux(<QueryBuilder />, setInitialState)

    let keysButton = await waitForElement(() => getByText('tk1'))

    fireEvent.click(keysButton)

    const keyMenuItems = await waitForElement(() =>
      getAllByTestId(/dropdown--item/)
    )

    expect(keyMenuItems.length).toBe(2)

    const tk2 = getByTestId('dropdown--item tk2')

    fireEvent.click(tk2)

    keysButton = await waitForElement(() =>
      getByTestId('tag-selector--dropdown-button')
    )

    expect(keysButton.innerHTML.includes('tk2')).toBe(true)
    expect(queryAllByTestId(/dropdown--item/).length).toBe(0)
  })

  it('can select a tag value', async () => {
    const {getByText, getByTestId, queryAllByTestId} = renderWithRedux(
      <QueryBuilder />,
      setInitialState
    )

    const tagValue = await waitForElement(() => getByText('tv1'))

    fireEvent.click(tagValue)

    await waitForElement(() => getByTestId('tag-selector--container 1'))

    expect(queryAllByTestId(/tag-selector--container/).length).toBe(2)
  })
})
