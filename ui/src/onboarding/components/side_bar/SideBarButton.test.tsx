// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import SideBarButton from 'src/onboarding/components/side_bar/SideBarButton'
import {ComponentColor} from 'src/clockface'

const onClick = jest.fn(() => {})

const setup = (override?) => {
  const props = {
    key: 'key',
    text: 'text',
    titleText: 'titleText',
    color: ComponentColor.Secondary,
    onClick,
    ...override,
  }

  const wrapper = shallow(<SideBarButton {...props} />)

  return {wrapper}
}

describe('SideBarButton', () => {
  describe('rendering', () => {
    it('renders! wee!', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
      expect(wrapper).toMatchSnapshot()
    })
  })
})
