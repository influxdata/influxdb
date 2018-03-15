import React from 'react'
import {shallow} from 'enzyme'

import {isUUIDv4} from 'src/utils/stringValidators'

import KapacitorRulesTable from 'src/kapacitor/components/KapacitorRulesTable'

import {source, kapacitorRules} from 'test/resources'

const setup = () => {
  const props = {
    source,
    rules: kapacitorRules,
    onDelete: () => {},
    onChangeRuleStatus: () => {},
  }

  const wrapper = shallow(<KapacitorRulesTable {...props} />)

  return {
    wrapper,
    props,
  }
}

describe('Kapacitor.Components.KapacitorRulesTable', () => {
  describe('rendering', () => {
    it('renders KapacitorRulesTable', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })

    it('renders each row with key that is a UUIDv4', () => {
      const {wrapper} = setup()
      wrapper
        .find('tbody')
        .children()
        .forEach(child => expect(isUUIDv4(child.key())).toEqual(true))
    })
  })
})
