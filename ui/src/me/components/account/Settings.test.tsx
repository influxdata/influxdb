// Libraries
import React from 'react'
import {mount} from 'enzyme'

// Components
import {Settings} from 'src/me/components/account/Settings'
import {me} from '../../mockUserData'

const setup = (override?) => {
  const props = {
    me,
    ...override,
  }

  const wrapper = mount(<Settings {...props} />)

  return {wrapper}
}

describe('Account', () => {
  describe('rendering', () => {
    it('renders!', () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
      expect(wrapper).toMatchSnapshot()
    })

    it('displays the users info by default', () => {
      const {wrapper} = setup()

      const nameInput = wrapper.find({'data-test': 'nameInput'})
      expect(nameInput.props().value).toBe(me.name)
    })
  })
})
