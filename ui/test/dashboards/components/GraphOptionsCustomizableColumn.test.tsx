import React from 'react'

import GraphOptionsCustomizableColumn from 'src/dashboards/components/GraphOptionsCustomizableColumn'
import InputClickToEdit from 'src/shared/components/InputClickToEdit'

import {shallow} from 'enzyme'

const setup = (override = {}) => {
  const props = {
    displayName: '',
    internalName: '',
    onColumnRename: () => {},
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

      expect(label.exists()).toBe(true)
      expect(input.exists()).toBe(true)
    })

    describe('when there is an internalName', () => {
      it('displays the value', () => {
        const internalName = 'test'
        const {wrapper} = setup({internalName})
        const label = wrapper.find('div').last()
        expect(label.exists()).toBe(true)
        expect(label.children().contains(internalName)).toBe(true)
      })
    })
  })

  describe('instance methods', () => {
    describe('#handleColumnRename', () => {
      it('calls onColumnRename once', () => {
        const onColumnRename = jest.fn()
        const internalName = 'test'
        const {instance} = setup({onColumnRename, internalName})
        const rename = 'TEST'

        instance.handleColumnRename(rename)

        expect(onColumnRename).toHaveBeenCalledTimes(1)
        expect(onColumnRename).toHaveBeenCalledWith({
          displayName: rename,
          internalName,
        })
      })
    })
  })
})
