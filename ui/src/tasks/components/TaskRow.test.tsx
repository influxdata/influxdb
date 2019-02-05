// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {TaskRow} from 'src/tasks/components/TaskRow'

// Types
import {Label} from 'src/clockface'

// Constants
import {tasks, withRouterProps} from 'mocks/dummyData'

const setup = (override = {}) => {
  const props = {
    ...withRouterProps,
    task: tasks[0],
    onActivate: jest.fn(),
    onDelete: jest.fn(),
    onClone: jest.fn(),
    onSelect: jest.fn(),
    onEditLabels: jest.fn(),
    ...override,
  }

  const wrapper = shallow(<TaskRow {...props} />)

  return {wrapper}
}

describe('Tasks.Components.TaskRow', () => {
  it('renders', () => {
    const {wrapper} = setup()
    expect(wrapper.exists()).toBe(true)
    expect(wrapper).toMatchSnapshot()
  })

  describe('if task has labels', () => {
    it('renders with labels', () => {
      const {wrapper} = setup({task: tasks[1]})

      const labelContainer = wrapper.find(Label.Container)
      const labels = wrapper.find(Label)

      expect(labelContainer.exists()).toBe(true)
      expect(labels.length).toBe(tasks[1].labels.length)
    })
  })
})
