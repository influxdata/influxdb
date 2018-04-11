import React from 'react'
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

describe('CustomTimeRangeDropdown', () => {
  it('renders correctly', () => {
    const wrapper = setup()

    expect(wrapper.exists()).toBe(true)
  })
})
