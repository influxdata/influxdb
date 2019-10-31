// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import ScraperList from 'src/scrapers/components/ScraperList'

// Constants
import {scraperTargets} from 'mocks/dummyData'

const setup = (override?) => {
  const props = {
    scrapers: scraperTargets,
    emptyState: <></>,
    onDeleteScraper: jest.fn(),
    onUpdateScraper: jest.fn(),
    ...override,
  }

  const wrapper = shallow(<ScraperList {...props} />)

  return {wrapper}
}

describe('ScraperList', () => {
  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })
  })
})
