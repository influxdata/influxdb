// Libraries
import React from 'react'
import {renderWithRedux} from 'src/mockState'

// Components
import {TaskCard} from 'src/tasks/components/TaskCard'

// Constants
import {tasks, withRouterProps} from 'mocks/dummyData'

const task = tasks[1] // The 2nd task mock has labels on it

const setup = (override = {}) => {
  const props = {
    ...withRouterProps,
    task,
    onActivate: jest.fn(),
    onDelete: jest.fn(),
    onClone: jest.fn(),
    onSelect: jest.fn(),
    onRunTask: jest.fn(),
    onFilterChange: jest.fn(),
    onUpdate: jest.fn(),
    onAddTaskLabel: jest.fn(),
    onRemoveTaskLabel: jest.fn(),
    onCreateLabel: jest.fn(),
    labels: [], // all labels
    ...override,
  }

  return renderWithRedux(<TaskCard {...props} />)
}

describe('Tasks.Components.TaskCard', () => {
  describe('if task has labels', () => {
    it('renders with labels', () => {
      const {getAllByTestId} = setup()

      const labels = getAllByTestId(/label--pill /)

      expect(labels.length).toEqual(task.labels.length)
    })
  })
})
