import React from 'react'
import {shallow} from 'enzyme'

import TasksTable from 'src/kapacitor/components/TasksTable'

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
})
