import React from 'react'

import {shallow} from 'enzyme'

import GraphOptionsTimeFormat from 'src/dashboards/components/GraphOptionsTimeFormat'
import {Dropdown} from 'src/shared/components/Dropdown'
import QuestionMarkTooltip from 'src/shared/components/QuestionMarkTooltip'
import {
  TIME_FORMAT_CUSTOM,
  TIME_FORMAT_TOOLTIP_LINK,
} from 'src/dashboards/constants'

const setup = (override = {}) => {
  const props = {
    onTimeFormatChange: () => {},
    timeFormat: '',
    ...override,
  }

  return shallow(<GraphOptionsTimeFormat {...props} />)
}

describe('Dashboards.Components.GraphOptionsTimeFormat', () => {
  describe('rendering', () => {
    describe('when it is not a custom format', () => {
      it('renders only a dropdown', () => {
        const wrapper = setup()
        const input = wrapper.find({'data-test': 'custom-time-format'})

        expect(wrapper.find(Dropdown).exists()).toBe(true)
        expect(wrapper.find(QuestionMarkTooltip).exists()).toBe(false)
        expect(input.exists()).toBe(false)
      })
    })

    describe('when state custom format is true', () => {
      it('renders all components', () => {
        const wrapper = setup()

        wrapper.setState({customFormat: true})

        const label = wrapper.find('label')
        const link = label.find('a')
        const input = wrapper.find({'data-test': 'custom-time-format'})

        expect(wrapper.find(Dropdown).exists()).toBe(true)
        expect(label.exists()).toBe(true)
        expect(link.exists()).toBe(true)
        expect(link.prop('href')).toBe(TIME_FORMAT_TOOLTIP_LINK)
        expect(link.find(QuestionMarkTooltip).exists()).toBe(true)
        expect(input.exists()).toBe(true)
      })
    })

    describe('when format is not from the dropdown options', () => {
      it('renders all components with "custom" selected in dropdown', () => {
        const timeFormat = 'mmmmmmm'
        const wrapper = setup({timeFormat})
        const dropdown = wrapper.find(Dropdown)
        const input = wrapper.find({'data-test': 'custom-time-format'})
        const label = wrapper.find('label')
        const link = label.find('a')

        expect(dropdown.prop('selected')).toBe(TIME_FORMAT_CUSTOM)
        expect(input.exists()).toBe(true)
        expect(input.prop('value')).toBe(timeFormat)
        expect(link.exists()).toBe(true)
        expect(link.find(QuestionMarkTooltip).exists()).toBe(true)
      })
    })
  })

  describe('instance methods', () => {
    describe('#handleChooseFormat', () => {
      describe('when input is custom', () => {
        it('sets the state custom format to true', () => {
          const instance = setup().instance() as GraphOptionsTimeFormat

          instance.handleChooseFormat({text: TIME_FORMAT_CUSTOM})
          expect(instance.state.customFormat).toBe(true)
        })
      })

      describe('when input is not custom', () => {
        it('sets the state custom format to false and calls onTimeFormatChange', () => {
          const onTimeFormatChange = jest.fn()
          const instance = setup({
            onTimeFormatChange,
          }).instance() as GraphOptionsTimeFormat

          instance.handleChooseFormat({text: 'blah'})
          expect(instance.state.customFormat).toBe(false)
          expect(onTimeFormatChange).toBeCalledWith('blah')
          expect(onTimeFormatChange).toHaveBeenCalledTimes(1)
        })
      })
    })

    describe('#handleChangeFormat', () => {
      it('sets state format to format and calls onTimeFormatChange', () => {
        const onTimeFormatChange = jest.fn()
        const format = 'mmmmmm'
        const instance = setup({
          onTimeFormatChange,
        }).instance() as GraphOptionsTimeFormat

        instance.handleChangeFormat({target: {value: format}})
        expect(instance.state.format).toBe(format)
        expect(onTimeFormatChange).toBeCalledWith(format)
        expect(onTimeFormatChange).toHaveBeenCalledTimes(1)
      })
    })
  })
})
