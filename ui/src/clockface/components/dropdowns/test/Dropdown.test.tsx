import React from 'react'
import {mount} from 'enzyme'

import Dropdown from 'src/clockface/components/dropdowns/Dropdown'

describe('Dropdown', () => {
  let wrapper

  const wrapperSetup = (override = {}) => {
    const props = {
      selectedID: 'jimmy',
      onChange: () => {},
      children: null,
      ...override,
    }

    return mount(<Dropdown {...props} />)
  }

  const childSetup = (override = {}) => {
    const props = {
      id: 'jimmy',
      value: 'jimmy',
      children: 'jimmy',
      ...override,
    }

    return <Dropdown.Item {...props} />
  }

  const childA = childSetup()

  const childB = childSetup({
    id: 'johnny',
    value: 'johnny',
    children: 'johnny',
  })

  const children = [childA, childB]

  describe('collapsed', () => {
    beforeEach(() => {
      wrapper = wrapperSetup({
        selectedID: 'johnny',
        children,
      })
    })

    it('can hide menu items', () => {
      expect(wrapper.find(Dropdown.Item)).toHaveLength(0)
    })
  })

  describe('expanded', () => {
    beforeEach(() => {
      wrapper = wrapperSetup({
        selectedID: 'johnny',
        children,
      })

      wrapper.find('button').simulate('click')
    })

    it('can display menu items', () => {
      expect(wrapper.find(Dropdown.Item)).toHaveLength(2)
    })

    it('can set the selectedID', () => {
      const actualProps = wrapper
        .find(Dropdown.Item)
        .find({selected: true})
        .props()

      const expectedProps = expect.objectContaining({
        id: 'johnny',
        value: 'johnny',
      })

      expect(actualProps).toEqual(expectedProps)
    })
  })

  describe('when no children are present', () => {
    const errorLog = console.error

    beforeEach(() => {
      console.error = jest.fn(() => {})
    })

    afterEach(() => {
      console.error = errorLog
    })

    it('throws error', () => {
      expect(() => {
        wrapperSetup({children: null})
      }).toThrow(
        'Dropdowns require at least 1 child element. We recommend using Dropdown.Item and/or Dropdown.Divider.'
      )
    })
  })
})
