// Libraries
import React from 'react'
import {shallow, mount} from 'enzyme'

// Components
import {Input, FormElement} from '@influxdata/clockface'
import ConfigFieldSwitcher from 'src/dataLoaders/components/configureStep/streaming/ConfigFieldSwitcher'
import ArrayFormElement from 'src/dataLoaders/components/configureStep/streaming/ArrayFormElement'
import URIFormElement from 'src/shared/components/URIFormElement'

// Types
import {ConfigFieldType} from 'src/types'
import {TelegrafPluginInputCpu} from '@influxdata/influx'

const setup = (override = {}, shouldMount = false) => {
  const props = {
    fieldName: '',
    fieldType: ConfigFieldType.String,
    onChange: jest.fn(),
    addTagValue: jest.fn(),
    removeTagValue: jest.fn(),
    index: 0,
    value: '',
    isRequired: true,
    onSetConfigArrayValue: jest.fn(),
    telegrafPluginName: TelegrafPluginInputCpu.NameEnum.Cpu,
    ...override,
  }

  const wrapper = shouldMount
    ? mount(<ConfigFieldSwitcher {...props} />)
    : shallow(<ConfigFieldSwitcher {...props} />)

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

    describe('if not required', () => {
      it('optional is displayed as help text', () => {
        const fieldName = 'yo'
        const fieldType = ConfigFieldType.String
        const value = ''
        const {wrapper} = setup({
          fieldName,
          fieldType,
          isRequired: false,
          value,
        })

        const input = wrapper.find(Input)
        const formElement = wrapper.find(FormElement)

        expect(wrapper.exists()).toBe(true)
        expect(input.exists()).toBe(true)
        expect(formElement.prop('helpText')).toBe('optional')
      })
    })
  })

  describe('if type is array', () => {
    it('renders an array input', () => {
      const fieldName = ['yo']
      const fieldType = ConfigFieldType.StringArray
      const value = []
      const {wrapper} = setup({fieldName, fieldType, value}, true)

      const input = wrapper.find(ArrayFormElement)
      const formElement = wrapper.find(FormElement).first()

      expect(input.exists()).toBe(true)
      expect(formElement.prop('helpText')).toBe('')
    })

    describe('if not required', () => {
      const fieldName = ['yo']
      const value = []
      const fieldType = ConfigFieldType.StringArray
      const {wrapper} = setup(
        {fieldName, fieldType, value, isRequired: false},
        true
      )

      const input = wrapper.find(ArrayFormElement)
      const formElement = wrapper.find(FormElement).first()

      expect(wrapper.exists()).toBe(true)
      expect(input.exists()).toBe(true)
      expect(formElement.prop('helpText')).toBe('optional')
    })
  })

  describe('if type is uri', () => {
    it('renders a uri input ', () => {
      const fieldName = ['http://google.com']
      const fieldType = ConfigFieldType.Uri
      const value = ''
      const {wrapper} = setup({fieldName, fieldType, value}, true)

      const input = wrapper.find(URIFormElement)

      expect(wrapper.exists()).toBe(true)
      expect(input.exists()).toBe(true)
    })

    describe('if not required', () => {
      it('optional is displayed as help text', () => {
        const fieldName = ['http://google.com']
        const fieldType = ConfigFieldType.Uri
        const value = ''
        const {wrapper} = setup(
          {fieldName, fieldType, value, isRequired: false},
          true
        )

        const input = wrapper.find(URIFormElement)

        expect(wrapper.exists()).toBe(true)
        expect(input.exists()).toBe(true)
      })
    })
  })
})
