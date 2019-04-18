// Libraries
import React from 'react'
import {renderWithRedux} from 'src/mockState'

// Components
import MemberList from 'src/members/components/MemberList'

// Constants
import {resouceOwner} from 'src/members/dummyData'

const setup = (override?) => {
  const props = {
    members: resouceOwner,
    emptyState: <></>,
    ...override,
  }

  const wrapper = renderWithRedux(<MemberList {...props} />)

  return {wrapper}
}

describe('MemberList', () => {
  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()
      expect(wrapper).toMatchSnapshot()
    })
  })
})
