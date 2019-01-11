// Libraries
import React from 'react'
import {mount} from 'enzyme'

// Components
import ViewTokenOverlay from 'src/me/components/account/ViewTokenOverlay'

// Fixtures
import {authorization as auth} from 'src/authorizations/apis/__mocks__/data'

const setup = (override?) => {
  const props = {
    auth,
    ...override,
  }

  const wrapper = mount(<ViewTokenOverlay {...props} />)

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
