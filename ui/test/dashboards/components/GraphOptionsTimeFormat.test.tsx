import React from 'react'
import GraphOptionsTimeFormat from 'src/dashboards/components/GraphOptionsTimeFormat'
import {Dropdown} from 'src/shared/components/Dropdown'
import DropdownMenu from 'src/shared/components/DropdownMenu'
import QuestionMarkTooltip from 'src/shared/components/QuestionMarkTooltip'
import InputClickToEdit from 'src/shared/components/InputClickToEdit'
import {shallow} from 'enzyme'

const setup = (override = {}) => {
  const props = {
    timeFormat: '',
    onTimeFormatChange: () => {},
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

    describe('when it is a custom format', () => {
      it('renders all components', () => {
        const wrapper = setup()

        wrapper.setState({customFormat: true})

        expect(wrapper.find(Dropdown).exists()).toBe(true)
        expect(wrapper.find(QuestionMarkTooltip).exists()).toBe(true)
        expect(wrapper.find(InputClickToEdit).exists()).toBe(true)
      })
    })
  })

  describe('instance methods', () => {
    describe('#handleChooseFormat', () => {
      describe('when input is custom', () => {
        it('sets the state custom format to true', () => {
          const instance = setup().instance() as GraphOptionsTimeFormat

          instance.handleChooseFormat({text: 'Custom'})
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
