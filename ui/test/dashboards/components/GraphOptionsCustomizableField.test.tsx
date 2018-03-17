import React from 'react'

import GraphOptionsCustomizableField from 'src/dashboards/components/GraphOptionsCustomizableField'
import InputClickToEdit from 'src/shared/components/InputClickToEdit'

import {shallow} from 'enzyme'

const setup = (override = {}) => {
  const props = {
    internalName: '',
    displayName: '',
    visible: true,
    onFieldUpdate: () => {},
    ...override,
  }

  const wrapper = shallow(<GraphOptionsCustomizableField {...props} />)
  const instance = wrapper.instance() as GraphOptionsCustomizableField

  return {wrapper, props, instance}
}

describe('Dashboards.Components.GraphOptionsCustomizableField', () => {
  describe('rendering', () => {
    it('displays both label div and InputClickToEdit', () => {
      const {wrapper} = setup()
      const label = wrapper.find('div').last()
      const input = wrapper.find(InputClickToEdit)
      const icon = wrapper.find('span')

      expect(label.exists()).toBe(true)
      expect(icon.exists()).toBe(true)
      expect(input.exists()).toBe(true)
    })

    describe('when there is an internalName', () => {
      it('displays the value', () => {
        const internalName = 'test'
        const {wrapper} = setup({internalName})
        const label = wrapper.find('div').last()
        const icon = wrapper.find('span')

        expect(label.exists()).toBe(true)
        expect(label.children().contains(internalName)).toBe(true)
        expect(icon.exists()).toBe(true)
      })
    })

    describe('when visible is false', () => {
      it('displays disabled inputClickToEdit', () => {
        const visible = false
        const {wrapper} = setup({visible})
        const input = wrapper.find(InputClickToEdit)

        expect(input.prop('disabled')).toBe(!visible)
      })
    })
  })

  describe('instance methods', () => {
    describe('#handleFieldUpdate', () => {
      it('calls onFieldUpdate once with internalName, new name, and visible', () => {
        const onFieldUpdate = jest.fn()
        const internalName = 'test'
        const {instance, props: {visible}} = setup({
          onFieldUpdate,
          internalName,
        })
        const rename = 'TEST'

        instance.handleFieldRename(rename)

        expect(onFieldUpdate).toHaveBeenCalledTimes(1)
        expect(onFieldUpdate).toHaveBeenCalledWith({
          internalName,
          displayName: rename,
          visible,
        })
      })
    })

    describe('#handleToggleVisible', () => {
      it('calls onFieldUpdate once with !visible, internalName, and displayName', () => {
        const onFieldUpdate = jest.fn()
        const visible = true
        const {instance, props: {internalName, displayName}} = setup({
          visible,
          onFieldUpdate,
        })

        instance.handleToggleVisible()

        expect(onFieldUpdate).toHaveBeenCalledTimes(1)
        expect(onFieldUpdate).toHaveBeenCalledWith({
          internalName,
          displayName,
          visible: !visible,
        })
      })
    })
  })
})
