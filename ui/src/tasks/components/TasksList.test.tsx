// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import TasksList from 'src/tasks/components/TasksList'

// Types
import {Task} from 'src/types'

// Constants
import {tasks} from 'mocks/dummyData'

const setup = (override?) => {
  const props = {
    tasks,
    searchTerm: '',
    onActivate: oneTestFunction,
    onDelete: oneTestFunction,
    onCreate: secondTestFunction,
    onSelect: oneTestFunction,
    onImportTask: oneTestFunction,
    checkTaskLimits: secondTestFunction,
    ...override,
  }

  const wrapper = shallow(<TasksList {...props} />)

  return {wrapper}
}

const oneTestFunction = (tasks: Task) => {
  tasks[0].name = 'someone'
  return
}

const secondTestFunction = () => {
  return
}

describe('TasksList', () => {
  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })
  })
})
