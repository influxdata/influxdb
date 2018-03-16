import React from 'react'

import GraphOptionsCustomizableColumn from 'src/dashboards/components/GraphOptionsCustomizableColumn'
import InputClickToEdit from 'src/shared/components/InputClickToEdit'

import {shallow} from 'enzyme'

const setup = (override = {}) => {
  const props = {
    internalName: '',
    displayName: '',
    visible: true,
    onColumnUpdate: () => {},
    ...override,
  }

  const wrapper = shallow(<GraphOptionsCustomizableColumn {...props} />)
  const instance = wrapper.instance() as GraphOptionsCustomizableColumn

  return {wrapper, props, instance}
}

describe('Dashboards.Components.GraphOptionsCustomizableColumn', () => {
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
    describe('#handleColumnUpdate', () => {
      it('calls onColumnUpdate once with internalName, new name, and visible', () => {
        const onColumnUpdate = jest.fn()
        const internalName = 'test'
        const {instance, props: {visible}} = setup({
          onColumnUpdate,
          internalName,
        })
        const rename = 'TEST'

        instance.handleColumnRename(rename)

        expect(onColumnUpdate).toHaveBeenCalledTimes(1)
        expect(onColumnUpdate).toHaveBeenCalledWith({
          internalName,
          displayName: rename,
          visible,
        })
      })
    })

    describe('#handleToggleVisible', () => {
      it('calls onColumnUpdate once with !visible, internalName, and displayName', () => {
        const onColumnUpdate = jest.fn()
        const visible = true
        const {instance, props: {internalName, displayName}} = setup({
          visible,
          onColumnUpdate,
        })

        instance.handleToggleVisible()

        expect(onColumnUpdate).toHaveBeenCalledTimes(1)
        expect(onColumnUpdate).toHaveBeenCalledWith({
          internalName,
          displayName,
          visible: !visible,
        })
      })
    })
  })
})
