// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {Input, SelectableCard} from '@influxdata/clockface'
import StreamingSelector from 'src/dataLoaders/components/collectorsWizard/select/StreamingSelector'

// Constants
import {PLUGIN_BUNDLE_OPTIONS} from 'src/dataLoaders/constants/pluginConfigs'

// Mocks
import {buckets} from 'mocks/dummyData'

const setup = (override = {}) => {
  const selectedBucketName = buckets[0].name

  const props = {
    telegrafPlugins: [],
    pluginBundles: [],
    onTogglePluginBundle: jest.fn(),
    buckets,
    selectedBucketName,
    onSelectBucket: jest.fn(),
    ...override,
  }

  return shallow(<StreamingSelector {...props} />)
}

describe('Onboarding.Components.SelectionStep.StreamingSelector', () => {
  it('renders a filter input and plugin bundles', () => {
    const wrapper = setup()
    const cards = wrapper.find(SelectableCard)
    const filter = wrapper.find(Input)

    expect(cards.length).toBe(PLUGIN_BUNDLE_OPTIONS.length)
    expect(filter.exists()).toBe(true)
  })

  describe('if searchTerm is not empty', () => {
    it('filters the plugin bundles', () => {
      const wrapper = setup()
      const searchTerm = 'syste'
      wrapper.setState({searchTerm})

      const cards = wrapper.find(SelectableCard)
      expect(cards.length).toBe(1)
    })
  })
})
