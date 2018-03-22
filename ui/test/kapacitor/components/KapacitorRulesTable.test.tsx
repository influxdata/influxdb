import React from 'react'
import {shallow} from 'enzyme'

import KapacitorRulesTable from 'src/kapacitor/components/KapacitorRulesTable'
import {RuleRow} from 'src/kapacitor/components/KapacitorRulesTable'

import {source, kapacitorRules} from 'test/resources'

describe('Kapacitor.Components.KapacitorRulesTable', () => {
  describe('rendering', () => {
    const props = {
      source,
      rules: kapacitorRules,
      onDelete: () => {},
      onChangeRuleStatus: () => {},
    }

    it('renders KapacitorRulesTable', () => {
      const wrapper = shallow(<KapacitorRulesTable {...props} />)

      expect(wrapper.exists()).toBe(true)
    })
  })
})

describe('Kapacitor.Containers.KapacitorRulesTable.RuleRow', () => {
  const props = {
    source,
    rule: kapacitorRules[0],
    onDelete: () => {},
    onChangeRuleStatus: jest.fn(),
  }

  afterEach(() => {
    jest.clearAllMocks()
  })

  describe('rendering', () => {
    it('renders RuleRow', () => {
      const wrapper = shallow(<RuleRow {...props} />)

      expect(wrapper.exists()).toBe(true)
    })
  })

  describe('user interaction', () => {
    it('calls onChangeRuleStatus when checkbox is effectively clicked', () => {
      const wrapper = shallow(<RuleRow {...props} />)

      const checkbox = wrapper.find({type: 'checkbox'})
      checkbox.simulate('change')

      expect(props.onChangeRuleStatus).toHaveBeenCalledTimes(1)
      expect(props.onChangeRuleStatus).toHaveBeenCalledWith(kapacitorRules[0])
    })
  })
})
