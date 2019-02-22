import React from 'react'
import {fireEvent} from 'react-testing-library'
import {dashboard} from 'mocks/dummyData'
import {renderWithRedux} from 'src/mockState'

import Dashboards from 'src/organizations/components/Dashboards'

function setup(override?) {
  const props = {
    onChange: jest.fn(),
    orgName: '',
    orgID: '',
    ...override,
  }

  return renderWithRedux(<Dashboards {...props} />, s => ({
    ...s,
    links: [],
    dashboards: [],
    orgs: [],
    ...override,
  }))
}

describe('Dashboards', () => {
  const labelName = 'clickMe'

  const label = {
    id: '123',
    name: 'clickMe',
    properties: {
      color: '#ff0011',
      description: '',
    },
  }

  it('renders', () => {
    const dashOne = {...dashboard, name: 'Test Dash 1', id: '1'}
    const dashTwo = {...dashboard, name: 'Test Dash 2', id: '2'}
    const dashboards = [dashOne, dashTwo]
    const {getAllByTestId} = setup({dashboards})

    const expected = getAllByTestId('resource-card')
    expect(expected.length).toEqual(dashboards.length)
  })

  it('filters on label click', () => {
    const otherLabel = {
      ...label,
      id: '1',
      name: 'other',
    }

    const dashOne = {
      ...dashboard,
      name: 'Test Dash 1 ',
      id: '1',
      labels: [otherLabel],
    }

    const dashTwo = {
      ...dashboard,
      name: 'Test Dash 2',
      id: '2',
      labels: [label],
    }

    const dashboards = [dashOne, dashTwo]
    const {getAllByTestId, getByTestId} = setup({dashboards})

    const labelPill = getByTestId(`label--pill ${labelName}`)

    expect(getAllByTestId('resource-card')).toHaveLength(2)

    fireEvent.click(labelPill)

    expect(getAllByTestId('resource-card')).toHaveLength(1)
  })

  it('displays searchTerm on label click', () => {
    const dashOne = {
      ...dashboard,
      name: 'Test Dash 2',
      id: '1',
      labels: [label],
    }

    const dashboards = [dashOne]
    const {getByTestId} = setup({dashboards})

    const labelPill = getByTestId(`label--pill ${labelName}`)

    const emptyFilterField = getByTestId(
      `dashboards--filter-field`
    ) as HTMLInputElement
    expect(emptyFilterField.value).toEqual('')

    fireEvent.click(labelPill)

    const filterField = getByTestId(
      `dashboards--filter-field ${labelName}`
    ) as HTMLInputElement
    expect(filterField.value).toEqual(labelName)
  })
})
