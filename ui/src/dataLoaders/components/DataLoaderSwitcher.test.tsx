// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import DataLoaderSwitcher from 'src/dataLoaders/components/DataLoaderSwitcher'
import DataLoadersWizard from 'src/dataLoaders/components/DataLoadersWizard'
import CollectorsWizard from 'src/dataLoaders/components/collectorsWizard/CollectorsWizard'
import LineProtocolWizard from 'src/dataLoaders/components/lineProtocolWizard/LineProtocolWizard'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'

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
  it('renders data loaders wizard', () => {
    const {wrapper} = setup()

    const wizard = wrapper.find(DataLoadersWizard)

    expect(wrapper.exists()).toBe(true)
    expect(wizard.exists()).toBe(true)
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
