// Libraries
import React from 'react'
import {fireEvent} from 'react-testing-library'

// Utils
import {renderWithRedux} from 'src/mockState'

// Constants
import {tasks} from 'mocks/dummyData'

// Components
import OrgTasksPage from 'src/organizations/components/OrgTasksPage'

function setup(override) {
  const props = {
    tasks: [],
    orgName: '',
    orgID: '',
    onChange: jest.fn(),
    router: null,
    ...override.props,
  }

  return renderWithRedux(<OrgTasksPage {...props} />, s => ({
    ...s,
    orgs: [],
    tasks: {
      searchTerm: '',
      showInactive: true,
      ...override.state,
    },
  }))
}

describe('OrgsTasksPage', () => {
  it('renders', () => {
    const {getAllByTestId} = setup({props: {tasks}, state: {}})

    expect(getAllByTestId('task-card')).toHaveLength(tasks.length)
  })

  describe('labels click', () => {
    const labelName = 'clickMe'
    const taskOne = {
      ...tasks[0],
      labels: [
        {
          id: '123',
          name: labelName,
          properties: {
            color: '#FFAA99',
            description: '',
          },
        },
      ],
    }
    const filterTasks = [taskOne, tasks[1]]

    it('filters when label is clicked', () => {
      const labelName = 'clickMe'
      const taskOne = {
        ...tasks[0],
        labels: [
          {
            id: '123',
            name: labelName,
            properties: {
              color: '#FFAA99',
              description: '',
            },
          },
        ],
      }

      const filterTasks = [taskOne, tasks[1]]
      const {getAllByTestId, getByTestId} = setup({
        props: {tasks: filterTasks},
        state: {},
      })

      expect(getAllByTestId('task-card')).toHaveLength(2)

      const labelPill = getByTestId(`label--pill ${labelName}`)

      fireEvent.click(labelPill)

      expect(getAllByTestId('task-card')).toHaveLength(1)
    })

    it('displays label name in input when clicked', () => {
      const {getAllByTestId, getByTestId} = setup({
        props: {
          tasks: filterTasks,
        },
        state: {},
      })

      expect(getAllByTestId('task-card')).toHaveLength(2)

      const labelPill = getByTestId(`label--pill ${labelName}`)
      fireEvent.click(labelPill)

      const input = getByTestId(`search-widget`) as HTMLInputElement
      expect(input.value).toEqual(labelName)
    })
  })
})
