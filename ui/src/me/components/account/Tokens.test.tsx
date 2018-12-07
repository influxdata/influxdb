// Libraries
import React from 'react'
import {mount} from 'enzyme'

// Components
import {Tokens} from 'src/me/components/account/Tokens'
import TokenRow from 'src/me/components/account/TokenRow'

jest.mock('src/authorizations/apis')

const setup = (override?) => {
  const props = {
    authorizationsLink: 'api/v2/authorizations',
    ...override,
  }

  const wrapper = mount(<Tokens {...props} />)

  return {wrapper}
}

describe('Account', () => {
  describe('rendering', () => {
    it('renders!', () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
      expect(wrapper).toMatchSnapshot()
    })

    it('displays the list of tokens', done => {
      const {wrapper} = setup()

      process.nextTick(() => {
        wrapper.update()
        const rows = wrapper.find(TokenRow)
        expect(rows.length).toBe(2)
        done()
      })
    })
  })
})
