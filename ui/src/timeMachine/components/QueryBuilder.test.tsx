import React from 'react'
import {Route} from 'react-router'

import {renderWithReduxAndRouter} from 'src/mockState'
import {waitForElement, fireEvent} from 'react-testing-library'

import QueryBuilder from 'src/timeMachine/components/QueryBuilder'

jest.mock('src/timeMachine/apis/queryBuilder')

describe('QueryBuilder', () => {
  it('can select a bucket', async () => {
    const {
      getByTestId,
      getAllByTestId,
      queryAllByTestId,
      getByText,
    } = renderWithReduxAndRouter(
      <Route path="/orgs/:orgID/data-explorer" component={QueryBuilder} />,
      () => ({}),
      {
        route: '/orgs/039d995276a14000/data-explorer',
      }
    )

    const bucketsDropdownClosed = await waitForElement(() => getByText('b1'))

    fireEvent.click(bucketsDropdownClosed)

    const bucketItems = getAllByTestId(/dropdown--item/)

    expect(bucketItems.length).toBe(2)

    const b2 = getByTestId('dropdown--item b2')

    fireEvent.click(b2)

    const closedDropdown = await waitForElement(() =>
      getByTestId('buckets--button')
    )

    expect(closedDropdown.textContent).toBe('b2')
    expect(queryAllByTestId(/dropdown--item/).length).toBe(0)
  })

  it('can select a tag', async () => {
    const {
      getByText,
      getByTestId,
      getAllByTestId,
      queryAllByTestId,
    } = renderWithReduxAndRouter(
      <Route path="/orgs/:orgID/data-explorer" component={QueryBuilder} />,
      () => ({}),
      {
        route: '/orgs/039d995276a14000/data-explorer',
      }
    )

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
    const {getByText, getByTestId, queryAllByTestId} = renderWithReduxAndRouter(
      <Route path="/orgs/:orgID/data-explorer" component={QueryBuilder} />,
      () => ({}),
      {
        route: '/orgs/039d995276a14000/data-explorer',
      }
    )

    const tagValue = await waitForElement(() => getByText('tv1'))

    fireEvent.click(tagValue)

    await waitForElement(() => getByTestId('tag-selector--container 1'))

    expect(queryAllByTestId(/tag-selector--container/).length).toBe(2)
  })
})
