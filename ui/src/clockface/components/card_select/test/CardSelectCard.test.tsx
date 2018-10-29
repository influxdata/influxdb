import React from 'react'
import {shallow} from 'enzyme'

import CardSelectCard from 'src/clockface/components/card_select/CardSelectCard'

describe('Card Select Card', () => {
  let wrapper

  const wrapperSetup = (override = {}) => {
    const props = {
      id: 'card_id',
      label: 'Card Label',
      name: undefined,
      image: undefined,
      checked: undefined,
      disabled: undefined,
      onClick: undefined,
      ...override,
    }

    return shallow(<CardSelectCard {...props} />)
  }

  beforeEach(() => {
    jest.resetAllMocks()
    wrapper = wrapperSetup()
  })

  it('mounts without exploding', () => {
    expect(wrapper).toHaveLength(1)
  })

  it('renders a div with data-toggle attribute', () => {
    const toggle = wrapper
      .find('div')
      .filterWhere(div => div.prop('data-toggle'))
    expect(toggle).toHaveLength(1)
    expect(toggle.prop('data-toggle')).toBe('card_toggle')
  })

  it('renders one label', () => {
    expect(wrapper.find('label')).toHaveLength(1)
  })

  it('renders one input field of type checkbox', () => {
    const field = wrapper.find('input')
    expect(field).toHaveLength(1)
    expect(field.prop('type')).toBe('checkbox')
  })

  it('matches snapshot with minimal props', () => {
    expect(wrapper).toMatchSnapshot()
  })

  describe('with image', () => {
    beforeEach(() => {
      jest.resetAllMocks()
      wrapper = wrapperSetup({image: 'URL'})
    })

    it('renders an image tag if passed an image source', () => {
      expect(wrapper.find('img')).toHaveLength(1)
    })

    it('matches snapshot when provided image source', () => {
      expect(wrapper).toMatchSnapshot()
    })
  })
})
