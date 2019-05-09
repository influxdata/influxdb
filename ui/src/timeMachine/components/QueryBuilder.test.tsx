import React from 'react'

import {renderWithRedux} from 'src/mockState'
import {waitForElement, fireEvent} from 'react-testing-library'

import QueryBuilder from 'src/timeMachine/components/QueryBuilder'

jest.mock('src/timeMachine/apis/queryBuilder')

const setInitialState = state => {
  return {
    ...state,
    orgs: {
      org: {
        id: 'foo',
      },
    },
  }
}

describe('QueryBuilder', () => {
  it('can select a bucket', async () => {
    const {getByTestId} = renderWithRedux(<QueryBuilder />, setInitialState)

    const b2 = await waitForElement(() => getByTestId('selector-list b2'))

    fireEvent.click(b2)

    expect(b2.className).toContain('selected')
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

    const keyMenuItems = getAllByTestId(/dropdown--item/)

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
