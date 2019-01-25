// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import StreamingSelector from 'src/dataLoaders/components/selectionStep/StreamingSelector'
import CardSelectCard from 'src/clockface/components/card_select/CardSelectCard'
import {Input} from 'src/clockface'

// Constants
import {PLUGIN_BUNDLE_OPTIONS} from 'src/dataLoaders/constants/pluginConfigs'

const setup = (override = {}) => {
  const props = {
    telegrafPlugins: [],
    pluginBundles: [],
    onTogglePluginBundle: jest.fn(),
    buckets: [],
    bucket: '',
    selectedBucket: '',
    onSelectBucket: jest.fn(),
    ...override,
  }

  return shallow(<StreamingSelector {...props} />)
}

describe('Onboarding.Components.SelectionStep.StreamingSelector', () => {
  it('renders a filter input and plugin bundles', () => {
    const wrapper = setup()
    const cards = wrapper.find(CardSelectCard)
    const filter = wrapper.find(Input)

    expect(cards.length).toBe(PLUGIN_BUNDLE_OPTIONS.length)
    expect(filter.exists()).toBe(true)
  })

  describe('if searchTerm is not empty', () => {
    it('filters the plugin bundles', async () => {
      const wrapper = setup()
      const searchTerm = 'syste'
      wrapper.setState({searchTerm})

      const cards = wrapper.find(CardSelectCard)
      expect(cards.length).toBe(1)
    })
  })
})
