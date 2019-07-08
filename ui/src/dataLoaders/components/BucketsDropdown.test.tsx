// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import BucketsDropdown from 'src/dataLoaders/components/BucketsDropdown'
import {Dropdown, ComponentStatus} from 'src/clockface'
import {bucket} from 'mocks/dummyData'

// Mocks

const setup = (override = {}) => {
  const props = {
    selectedBucketID: '',
    buckets: [],
    onSelectBucket: jest.fn(),
    ...override,
  }

  const wrapper = shallow(<BucketsDropdown {...props} />)

  return {wrapper}
}

describe('DataLoading.Components.BucketsDropdown', () => {
  describe('when no buckets', () => {
    it('renders disabled dropdown', () => {
      const {wrapper} = setup()

      const dropdown = wrapper.find(Dropdown)

      expect(wrapper.exists()).toBe(true)
      expect(dropdown.exists()).toBe(true)
      expect(dropdown.prop('status')).toBe(ComponentStatus.Disabled)
    })
  })

  describe('if buckets', () => {
    it('renders dropdown', () => {
      const buckets = [bucket]
      const {wrapper} = setup({buckets})

      const dropdown = wrapper.find(Dropdown)

      expect(wrapper.exists()).toBe(true)
      expect(dropdown.exists()).toBe(true)
      expect(dropdown.prop('status')).toBe(ComponentStatus.Default)
    })
  })
})
