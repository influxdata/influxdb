import React from 'react'
import {shallow} from 'enzyme'

import KapacitorRulesTable from 'src/kapacitor/components/KapacitorRulesTable'

import {source, kapacitorRules} from 'test/resources'

const setup = () => {
  const props = {
    source,
    rules: kapacitorRules,
    onDelete: () => {},
    onChangeRuleStatus: () => {}
  }

  const wrapper = shallow(<KapacitorRulesTable {...props} />)

  return {
    wrapper,
    props
  }
}

describe('Kapacitor.Components.KapacitorRulesTable', () => {
  describe('rendering', () => {
    it('renders KapacitorRulesTable', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })
  })
})
