import React from 'react'
import {shallow} from 'enzyme'

import TasksTable from 'src/kapacitor/components/TasksTable'

import {source, kapacitorRules} from 'test/resources'

const setup = () => {
  const props = {
    source,
    tasks: kapacitorRules,
    onDelete: () => {},
    onChangeRuleStatus: () => {}
  }

  const wrapper = shallow(<TasksTable {...props} />)

  return {
    wrapper,
    props
  }
}

describe('Kapacitor.Components.TasksTable', () => {
  describe('rendering', () => {
    it('renders the TasksTable', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })
  })
})
