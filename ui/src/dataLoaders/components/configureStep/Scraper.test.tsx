// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {ScraperTarget} from 'src/dataLoaders/components/configureStep/ScraperTarget'

const setup = (override = {}) => {
  const props = {
    bucket: '',
    buckets: [],
    onSelectBucket: jest.fn(),
    onChangeURL: jest.fn(),
    url: '',
    ...override,
  }
  const wrapper = shallow(<ScraperTarget {...props} />)

  return {wrapper}
}

describe('ScraperTarget', () => {
  describe('rendering', () => {
    it('renders!', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)

      expect(wrapper).toMatchSnapshot()
    })
  })
})
