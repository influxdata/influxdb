// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import Account from 'src/me/components/account/Account'

const setup = (override?) => {
  const props = {
    params: {tab: 'settings'},
    ...override,
  }

  const wrapper = shallow(<Account {...props} />)

  return {wrapper}
}

describe('Account', () => {
  describe('rendering', () => {
    it('renders!', () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
      expect(wrapper).toMatchSnapshot()
    })
  })
})
