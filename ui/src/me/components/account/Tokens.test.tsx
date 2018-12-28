// Libraries
import React from 'react'
import {mount} from 'enzyme'

// Components
import {Tokens} from 'src/me/components/account/Tokens'
import TokenRow from 'src/me/components/account/TokenRow'
import ViewTokenModal from 'src/me/components/account/ViewTokenOverlay'

// Fixtures
import {authorization} from 'src/authorizations/apis/__mocks__/data'

jest.mock('src/authorizations/apis')

const setup = (override?) => {
  const props = {
    authorizationsLink: 'api/v2/authorizations',
    ...override,
  }

  const tokensWrapper = mount(<Tokens {...props} />)

  return {tokensWrapper}
}

describe('Account', () => {
  let wrapper
  beforeEach(done => {
    const {tokensWrapper} = setup()
    wrapper = tokensWrapper
    process.nextTick(() => {
      wrapper.update()
      done()
    })
  })

  describe('rendering', () => {
    it('renders!', () => {
      expect(wrapper.exists()).toBe(true)
      expect(wrapper).toMatchSnapshot()
    })

    it('displays the list of tokens', () => {
      const rows = wrapper.find(TokenRow)
      expect(rows.length).toBe(2)
    })
  })

  describe('user interaction', () => {
    describe('clicking the token description', () => {
      it('opens the ViewTokenModal', () => {
        const description = wrapper.find({
          'data-test': `token-description-${authorization.id}`,
        })
        description.simulate('click')
        wrapper.update()

        const modal = wrapper.find(ViewTokenModal)

        expect(modal.exists()).toBe(true)
      })
    })
  })
})
