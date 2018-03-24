import {shallow} from 'enzyme'
import React from 'react'

import GraphOptionsCustomizableField from 'src/dashboards/components/GraphOptionsCustomizableField'
import InputClickToEdit from 'src/shared/components/InputClickToEdit'

const setup = (override = {}) => {
  const props = {
    displayName: '',
    internalName: '',
    onFieldUpdate: () => {},
    visible: true,
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

    describe('when there is an displayName', () => {
      it('display name displayed by input click to edit', () => {
        const internalName = 'test'
        const displayName = 'TESTING'
        const {wrapper} = setup({internalName, displayName})
        const label = wrapper.find('div').last()
        const icon = wrapper.find('span')
        const input = wrapper.find(InputClickToEdit)

        expect(label.exists()).toBe(true)
        expect(label.children().contains(internalName)).toBe(true)
        expect(icon.exists()).toBe(true)
        expect(input.exists()).toBe(true)
        expect(input.prop('value')).toBe(displayName)
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
          internalName,
          onFieldUpdate,
        })
        const rename = 'TEST'

        instance.handleFieldRename(rename)

        expect(onFieldUpdate).toHaveBeenCalledTimes(1)
        expect(onFieldUpdate).toHaveBeenCalledWith({
          displayName: rename,
          internalName,
          visible,
        })
      })
    })

    describe('#handleToggleVisible', () => {
      it('calls onFieldUpdate once with !visible, internalName, and displayName', () => {
        const onFieldUpdate = jest.fn()
        const visible = true
        const {instance, props: {internalName, displayName}} = setup({
          onFieldUpdate,
          visible,
        })

        instance.handleToggleVisible()

        expect(onFieldUpdate).toHaveBeenCalledTimes(1)
        expect(onFieldUpdate).toHaveBeenCalledWith({
          displayName,
          internalName,
          visible: !visible,
        })
      })
    })
  })
})
