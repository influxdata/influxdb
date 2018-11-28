// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import OnboardingSideBar from 'src/onboarding/components/OnboardingSideBar'

import {dataSources} from 'mocks/dummyData'

const onClick = jest.fn(() => {})

const setup = (override?) => {
  const props = {
    title: 'title',
    visible: true,
    dataSources,
    onTabClick: onClick,
    ...override,
  }

  const wrapper = shallow(<OnboardingSideBar {...props} />)

  return {wrapper}
}

describe('OnboardingSideBar', () => {
  describe('rendering', () => {
    it('renders! wee!', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
      expect(wrapper).toMatchSnapshot()
    })
  })
})
