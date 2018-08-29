import React from 'react'
import moment from 'moment'
import {shallow} from 'enzyme'
import CustomTimeRangeDropdown from 'src/shared/components/CustomTimeRangeDropdown'

const setup = (overrides = {}) => {
  const props = {
    timeRange: {upper: 'now()', lower: '2017-10-24'},
    onApplyTimeRange: () => {},
    ...overrides,
  }

  return shallow(<CustomTimeRangeDropdown {...props} />)
}

describe('shared.Components.CustomTimeRangeDropdown', () => {
  describe('rendering', () => {
    it('renders correct time when now is selected', () => {
      const wrapper = setup()

      expect(wrapper.exists()).toBe(true)
      expect(wrapper.dive().text()).toContain(moment().format('MMM Do HH:mm'))
    })

    it('renders correct time when no upper is provided', () => {
      const wrapper = setup({timeRange: {lower: '2017-10-24'}})

      expect(wrapper.exists()).toBe(true)
      expect(wrapper.dive().text()).toContain(moment().format('MMM Do HH:mm'))
    })
  })
})
