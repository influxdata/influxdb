// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import ConfigFieldSwitcher from 'src/onboarding/components/configureStep/streaming/ConfigFieldSwitcher'
import ArrayFormElement from 'src/onboarding/components/configureStep/streaming/ArrayFormElement'
import URIFormElement from 'src/shared/components/URIFormElement'
import {Input} from 'src/clockface'

// Types
import {ConfigFieldType} from 'src/types/v2/dataLoaders'

const setup = (override = {}) => {
  const props = {
    fieldName: '',
    fieldType: ConfigFieldType.String,
    onChange: jest.fn(),
    addTagValue: jest.fn(),
    removeTagValue: jest.fn(),
    index: 0,
    value: '',
    ...override,
  }

  const wrapper = shallow(<ConfigFieldSwitcher {...props} />)

  return {wrapper}
}

describe('Onboarding.Components.ConfigureStep.Streaming.ConfigFieldSwitcher', () => {
  describe('if type is string', () => {
    it('renders an input', () => {
      const fieldName = 'yo'
      const fieldType = ConfigFieldType.String
      const {wrapper} = setup({fieldName, fieldType})

      const input = wrapper.find(Input)

      expect(wrapper.exists()).toBe(true)
      expect(input.exists()).toBe(true)
    })
  })
  describe('if type is array', () => {
    it('renders an array input', () => {
      const fieldName = ['yo']
      const fieldType = ConfigFieldType.StringArray
      const {wrapper} = setup({fieldName, fieldType})

      const input = wrapper.find(ArrayFormElement)

      expect(wrapper.exists()).toBe(true)
      expect(input.exists()).toBe(true)
    })
  })
  describe('if type is uri', () => {
    it('renders a uri input ', () => {
      const fieldName = ['http://google.com']
      const fieldType = ConfigFieldType.Uri
      const {wrapper} = setup({fieldName, fieldType})

      const input = wrapper.find(URIFormElement)

      expect(wrapper.exists()).toBe(true)
      expect(input.exists()).toBe(true)
    })
  })
})
