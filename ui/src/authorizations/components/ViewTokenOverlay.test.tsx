// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import ViewTokenOverlay from 'src/authorizations/components/ViewTokenOverlay'

// Fixtures
import {authorization as auth} from 'src/authorizations/apis/__mocks__/data'

const setup = (override?) => {
  const props = {
    auth,
    ...override,
  }

  const wrapper = shallow(<ViewTokenOverlay {...props} />)

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
