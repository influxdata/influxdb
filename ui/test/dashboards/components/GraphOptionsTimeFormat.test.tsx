import React from 'react'

import {shallow} from 'enzyme'

import GraphOptionsTimeFormat from 'src/dashboards/components/GraphOptionsTimeFormat'
import {Dropdown} from 'src/shared/components/Dropdown'
import InputClickToEdit from 'src/shared/components/InputClickToEdit'
import QuestionMarkTooltip from 'src/shared/components/QuestionMarkTooltip'
import {TIME_FORMAT_CUSTOM} from 'src/shared/constants/tableGraph'

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

        expect(wrapper.find(Dropdown).exists()).toBe(true)
        expect(wrapper.find(QuestionMarkTooltip).exists()).toBe(false)
        expect(wrapper.find(InputClickToEdit).exists()).toBe(false)
      })
    })

    describe('when state custom format is true', () => {
      it('renders all components', () => {
        const wrapper = setup()

        wrapper.setState({customFormat: true})

        expect(wrapper.find(Dropdown).exists()).toBe(true)
        expect(wrapper.find(QuestionMarkTooltip).exists()).toBe(true)
        expect(wrapper.find(InputClickToEdit).exists()).toBe(true)
      })
    })

    describe('when format is not from the dropdown options', () => {
      it('renders all components with "custom" selected in dropdown', () => {
        const timeFormat = 'mmmmmmm'
        const wrapper = setup({timeFormat})
        const dropdown = wrapper.find(Dropdown)
        const input = wrapper.find(InputClickToEdit)

        expect(dropdown.prop('selected')).toBe(TIME_FORMAT_CUSTOM)
        expect(input.exists()).toBe(true)
        expect(input.prop('value')).toBe(timeFormat)
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
        it('sets the state custom format to false', () => {
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
  })
})
