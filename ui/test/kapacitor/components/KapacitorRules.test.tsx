import React from 'react'
import {shallow} from 'enzyme'

import _ from 'lodash'

import KapacitorRules from 'src/kapacitor/components/KapacitorRules'

import {source, kapacitorRules} from 'test/resources'

const setup = () => {
  const props = {
    source,
    rules: kapacitorRules,
    hasKapacitor: true,
    loading: false,
    onDelete: () => {},
    onChangeRuleStatus: () => {},
  }

  const wrapper = shallow(<KapacitorRules {...props} />)

  return {
    wrapper,
    props,
  }
}

describe('Kapacitor.Containers.KapacitorRules', () => {
  describe('rendering', () => {
    it('renders KapacitorRules', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })

    it('renders KapacitorRulesTable', () => {
      const {wrapper} = setup()

      const kapacitorRulesTable = wrapper.find('KapacitorRulesTable')
      expect(kapacitorRulesTable.length).toEqual(1)

      const tasksTable = wrapper.find('TasksTable')
      expect(tasksTable.length).toEqual(1)
    })

    it('renders TasksTable', () => {
      const {wrapper} = setup()

      const tasksTable = wrapper.find('TasksTable')
      expect(tasksTable.length).toEqual(1)
    })

    it('renders each rule/task checkboxes with unique "id" attribute', () => {
      const {wrapper} = setup()

      const kapacitorRulesTableRowsIDs = wrapper
        .find('KapacitorRulesTable')
        .dive()
        .find('tbody')
        .children()
        .map(child => child.dive().find({type: 'checkbox'}).props().id)

      const tasksTableIDs = wrapper
        .find('TasksTable')
        .dive()
        .find('tbody')
        .children()
        .map(child => child.dive().find({type: 'checkbox'}).props().id)

      const allCheckboxesIDs = kapacitorRulesTableRowsIDs.concat(tasksTableIDs)

      const containsAnyDuplicate = arr => _.uniq(arr).length !== arr.length

      expect(containsAnyDuplicate(allCheckboxesIDs)).toEqual(false)
    })

    it('renders each rule/task table row label with unique "for" attribute', () => {
      const {wrapper} = setup()

      const kapacitorRulesTableRowsLabelFors = wrapper
        .find('KapacitorRulesTable')
        .dive()
        .find('tbody')
        .children()
        .map(child => child.dive().find('label').props().htmlFor)

      const tasksTableLabelFors = wrapper
        .find('TasksTable')
        .dive()
        .find('tbody')
        .children()
        .map(child => child.dive().find('label').props().htmlFor)

      const allCheckboxesLabelFors = kapacitorRulesTableRowsLabelFors.concat(
        tasksTableLabelFors
      )

      const containsAnyDuplicate = arr => _.uniq(arr).length !== arr.length

      expect(containsAnyDuplicate(allCheckboxesLabelFors)).toEqual(false)
    })
  })
})
