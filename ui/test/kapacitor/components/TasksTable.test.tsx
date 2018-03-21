import React from 'react'
import {shallow} from 'enzyme'

import TasksTable from 'src/kapacitor/components/TasksTable'
import {TaskRow} from 'src/kapacitor/components/TasksTable'

import {source, kapacitorRules} from 'test/resources'

describe('Kapacitor.Components.TasksTable', () => {
  describe('rendering', () => {
    const props = {
      source,
      tasks: kapacitorRules,
      onDelete: () => {},
      onChangeRuleStatus: () => {}
    }

    it('renders TasksTable', () => {
      const wrapper = shallow(<TasksTable {...props} />)

      expect(wrapper.exists()).toBe(true)
    })
  })

  describe('user interaction', () => {
    const props = {
      source,
      task: kapacitorRules[3],
      onDelete: () => {},
      onChangeRuleStatus: jest.fn(),
    }

    it('calls onChangeRuleStatus when checkbox is effectively clicked', () => {
      const wrapper = shallow(<TaskRow {...props} />)

      const checkbox = wrapper.find(({type:'checkbox'}))
      checkbox.simulate('change')

      expect(props.onChangeRuleStatus).toHaveBeenCalledTimes(1)
      expect(props.onChangeRuleStatus).toHaveBeenCalledWith(kapacitorRules[3])
    })
  })
})
