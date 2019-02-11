// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import MemberList from 'src/organizations/components/MemberList'

// Constants
import {resouceOwner} from 'src/organizations/dummyData'

const setup = (override?) => {
  const props = {
    members: resouceOwner,
    emptyState: <></>,
    ...override,
  }

  const wrapper = shallow(<MemberList {...props} />)

  return {wrapper}
}

describe('MemberList', () => {
  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
      expect(wrapper).toMatchSnapshot()
    })
  })
})
