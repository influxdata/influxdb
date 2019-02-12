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
    onChangeName: jest.fn(),
    url: '',
    name: '',
    ...override,
  }
  const wrapper = shallow(<ScraperTarget {...props} />)

  return {wrapper}
}

describe('ScraperTarget', () => {
  describe('rendering', () => {
    it('renders correctly with no bucket, url, name', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)

      expect(wrapper).toMatchSnapshot()
    })

    it('renders correctly with bucket', () => {
      const {wrapper} = setup({bucket: 'defbuck'})
      expect(wrapper.exists()).toBe(true)

      expect(wrapper).toMatchSnapshot()
    })

    it('renders correctly with url', () => {
      const {wrapper} = setup({url: 'http://url.com'})
      expect(wrapper.exists()).toBe(true)

      expect(wrapper).toMatchSnapshot()
    })

    it('renders correctly with name', () => {
      const {wrapper} = setup({name: 'MyScraper'})
      expect(wrapper.exists()).toBe(true)

      expect(wrapper).toMatchSnapshot()
    })
  })
})
