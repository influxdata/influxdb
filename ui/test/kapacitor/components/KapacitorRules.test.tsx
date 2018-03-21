import React from 'react'
import {shallow} from 'enzyme'

import _ from 'lodash'

import KapacitorRules from 'src/kapacitor/components/KapacitorRules'
import KapacitorRulesTable from 'src/kapacitor/components/KapacitorRulesTable'
import TasksTable from 'src/kapacitor/components/TasksTable'

import {source, kapacitorRules} from 'test/resources'

const props = {
  source,
  rules: kapacitorRules,
  hasKapacitor: true,
  loading: false,
  onDelete: () => {},
  onChangeRuleStatus: () => {},
}

describe('Kapacitor.Containers.KapacitorRules', () => {
  describe('rendering', () => {
    it('renders KapacitorRules', () => {
      const wrapper = shallow(<KapacitorRules {...props} />)

      expect(wrapper.exists()).toBe(true)
    })

    it('renders KapacitorRulesTable', () => {
      const wrapper = shallow(<KapacitorRules {...props} />)

      const kapacitorRulesTable = wrapper.find(KapacitorRulesTable)
      expect(kapacitorRulesTable.length).toEqual(1)

      const tasksTable = wrapper.find(TasksTable)
      expect(tasksTable.length).toEqual(1)
    })

    it('renders TasksTable', () => {
      const wrapper = shallow(<KapacitorRules {...props} />)

      const tasksTable = wrapper.find(TasksTable)
      expect(tasksTable.length).toEqual(1)
    })

    it('renders each rule/task checkboxes with unique "id" attribute', () => {
      const wrapper = shallow(<KapacitorRules {...props} />)

      const kapacitorRulesTableRowsIDs = wrapper
        .find(KapacitorRulesTable)
        .dive()
        .find('tbody')
        .children()
        .map(
          child =>
            child
              .dive()
              .find({type: 'checkbox'})
              .props().id
        )

      const tasksTableIDs = wrapper
        .find(TasksTable)
        .dive()
        .find('tbody')
        .children()
        .map(
          child =>
            child
              .dive()
              .find({type: 'checkbox'})
              .props().id
        )

      const allCheckboxesIDs = kapacitorRulesTableRowsIDs.concat(tasksTableIDs)

      const containsAnyDuplicate = arr => _.uniq(arr).length !== arr.length

      expect(containsAnyDuplicate(allCheckboxesIDs)).toEqual(false)
    })

    it('renders each rule/task table row label with unique "for" attribute', () => {
      const wrapper = shallow(<KapacitorRules {...props} />)

      const kapacitorRulesTableRowsLabelFors = wrapper
        .find(KapacitorRulesTable)
        .dive()
        .find('tbody')
        .children()
        .map(
          child =>
            child
              .dive()
              .find('label')
              .props().htmlFor
        )

      const tasksTableLabelFors = wrapper
        .find(TasksTable)
        .dive()
        .find('tbody')
        .children()
        .map(
          child =>
            child
              .dive()
              .find('label')
              .props().htmlFor
        )

      const allCheckboxesLabelFors = kapacitorRulesTableRowsLabelFors.concat(
        tasksTableLabelFors
      )

      const containsAnyDuplicate = arr => _.uniq(arr).length !== arr.length

      expect(containsAnyDuplicate(allCheckboxesLabelFors)).toEqual(false)
    })
  })
})
