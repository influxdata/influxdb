import React from 'react'
import {shallow} from 'enzyme'
import _ from 'lodash'

import TasksTable from 'src/kapacitor/components/TasksTable'
import {TaskRow} from 'src/kapacitor/components/TasksTable'

import {source, kapacitorRules} from 'test/resources'

const setup = (override = {}) => {
  const props = {
    source,
    tasks: kapacitorRules,
    onDelete: () => {},
    onChangeRuleStatus: () => {},
    ...override,
  }

  const wrapper = shallow(<TasksTable {...props} />)

  return {
    wrapper,
  }
}

describe('Kapacitor.Components.TasksTable', () => {
  describe('rendering', () => {
    it('renders TasksTable', () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
    })

    it('renders TaskRows alphabetically sorted by task name', () => {
      const {wrapper} = setup()
      const taskRows = wrapper.find(TaskRow)
      const actualNamesOrder = taskRows.map(taskRow => {
        const {name} = taskRow.prop('task')
        return name
      })

      const expectedNamesOrder = _.sortBy(kapacitorRules, r =>
        r.name.toLowerCase()
      ).map(r => r.name)

      expect(actualNamesOrder).toEqual(expectedNamesOrder)
    })

    describe('checkbox', () => {
      it('has the correct htmlFor its label', () => {
        const task = kapacitorRules[3]
        const tasks = [task]
        const {wrapper} = setup({tasks})

        const row = wrapper.find(TaskRow)
        const checkbox = row.dive().find({type: 'checkbox'})
        const label = row.dive().find('label')

        expect(checkbox.props().id).toBe(label.props().htmlFor)
      })
    })
  })

  describe('user interaction', () => {
    it('calls onChangeRuleStatus when checkbox is effectively clicked', () => {
      const task = kapacitorRules[3]
      const tasks = [task]
      const onChangeRuleStatus = jest.fn()

      const {wrapper} = setup({tasks, onChangeRuleStatus})

      const row = wrapper.find(TaskRow)
      const checkbox = row.dive().find({type: 'checkbox'})
      checkbox.simulate('change')

      expect(onChangeRuleStatus).toHaveBeenCalledTimes(1)
      expect(onChangeRuleStatus).toHaveBeenCalledWith(kapacitorRules[3])
    })
  })
})
