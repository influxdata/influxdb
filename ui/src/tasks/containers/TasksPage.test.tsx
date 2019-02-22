// Libraries
import React from 'react'
import {fireEvent} from 'react-testing-library'

// Utils
import {renderWithRedux} from 'src/mockState'

// Constants
import {tasks} from 'mocks/dummyData'

// Components
import TasksPage from 'src/tasks/containers/TasksPage'

import {Task as TaskApi} from '@influxdata/influx'

function setup(override) {
  const props = {
    router: null,
  }

  return renderWithRedux(<TasksPage {...props} />, s => ({
    ...s,
    orgs: [],
    tasks: {
      tasks: [],
      searchTerm: '',
      showInactive: true,
      dropdownOrgID: '1',
      ...override,
    },
  }))
}

describe('TasksPage', () => {
  it('renders', () => {
    const {getAllByTestId} = setup({showInactive: true, tasks})

    expect(getAllByTestId('task-row')).toHaveLength(tasks.length)
  })

  describe('active filtering', () => {
    const inactiveTask = {
      ...tasks[0],
      name: 'Task One',
      status: TaskApi.StatusEnum.Inactive,
      orgID: '1',
    }
    const activeTask = {
      ...tasks[1],
      name: 'Task Two',
      status: TaskApi.StatusEnum.Active,
      orgID: '1',
    }

    // Todo translate to e2e
    it('resets searchTerm and active task filtering', () => {
      const {getAllByTestId} = setup({
        dropdownOrgID: '1',
        showInactive: false,
        tasks: [inactiveTask, activeTask],
      })

      expect(getAllByTestId('task-row')).toHaveLength(2)
    })

    it('filters active on click', () => {
      const {getAllByTestId, getByTestId} = setup({
        dropdownOrgID: '1',
        showInactive: true,
        tasks: [inactiveTask, activeTask],
      })

      expect(getAllByTestId('task-row')).toHaveLength(2)

      const filterActiveToggle = getByTestId('slide-toggle')
      fireEvent.click(filterActiveToggle)

      expect(getAllByTestId('task-row')).toHaveLength(1)
    })

    // Todo translate to e2e
    it('filters active on active and searchTerm', () => {
      const labelName = 'clickMe'

      const activeLabels = [
        {
          id: '1111',
          name: labelName,
          properties: {
            color: '#FFAA99',
            description: '',
          },
        },
      ]

      const {getAllByTestId, getByTestId, getByText} = setup({
        dropdownOrgID: '1',
        showInactive: true,
        tasks: [
          inactiveTask,
          {...activeTask, name: 'Task Two', labels: activeLabels, id: '22'},
          {...activeTask, name: 'Task Three', id: '333'},
        ],
      })

      expect(getAllByTestId('task-row')).toHaveLength(3)

      const filterActiveToggle = getByTestId('slide-toggle')
      fireEvent.click(filterActiveToggle)

      expect(getAllByTestId('task-row')).toHaveLength(2)

      const labelPill = getByText(labelName)
      fireEvent.click(labelPill)

      expect(getAllByTestId('task-row')).toHaveLength(1)
    })
  })
})
