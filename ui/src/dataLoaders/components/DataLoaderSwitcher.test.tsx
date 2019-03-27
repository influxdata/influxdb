// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import DataLoaderSwitcher from 'src/dataLoaders/components/DataLoaderSwitcher'
import CreateScraperOverlay from 'src/organizations/components/CreateScraperOverlay'
import CollectorsWizard from 'src/dataLoaders/components/collectorsWizard/CollectorsWizard'
import LineProtocolWizard from 'src/dataLoaders/components/lineProtocolWizard/LineProtocolWizard'

// Types
import {DataLoaderType} from 'src/types'

const setup = (override = {}) => {
  const props = {
    type: DataLoaderType.Empty,
    onCompleteSetup: jest.fn(),
    visible: true,
    buckets: [],
    ...override,
  }

  const wrapper = shallow(<DataLoaderSwitcher {...props} />)

  return {wrapper}
}

describe('DataLoading.Components.DataLoaderSwitcher', () => {
  describe('if type is empty', () => {
    it('renders empty div', () => {
      const {wrapper} = setup({type: DataLoaderType.Empty})

      const emptyDiv = wrapper.find({'data-testid': 'data-loader-empty'})

      expect(wrapper.exists()).toBe(true)
      expect(emptyDiv.exists()).toBe(true)
    })
  })

  describe('if type is scraping', () => {
    it('renders create scraper overlay', () => {
      const {wrapper} = setup({type: DataLoaderType.Scraping})

      const overlay = wrapper.find(CreateScraperOverlay)

      expect(wrapper.exists()).toBe(true)
      expect(overlay.exists()).toBe(true)
    })
  })

  describe('if type is streaming', () => {
    it('renders collectors wizard', () => {
      const {wrapper} = setup({type: DataLoaderType.Streaming})

      const wizard = wrapper.find(CollectorsWizard)

      expect(wrapper.exists()).toBe(true)
      expect(wizard.exists()).toBe(true)
    })
  })

  describe('if type is line protocol', () => {
    it('renders line protocol wizard', () => {
      const {wrapper} = setup({type: DataLoaderType.LineProtocol})

      const wizard = wrapper.find(LineProtocolWizard)

      expect(wrapper.exists()).toBe(true)
      expect(wizard.exists()).toBe(true)
    })
  })
})
