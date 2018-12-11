// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import OnboardingSideBar from 'src/onboarding/components/OnboardingSideBar'
import {cpuPlugin, influxDB2Plugin} from 'mocks/dummyData'

const onClick = jest.fn(() => {})

const setup = (override?) => {
  const props = {
    title: 'title',
    visible: true,
    telegrafPlugins: [],
    onTabClick: onClick,
    ...override,
  }

  const wrapper = shallow(<OnboardingSideBar {...props} />)

  return {wrapper}
}

describe('OnboardingSideBar', () => {
  describe('rendering', () => {
    it('renders! wee!', () => {
      const {wrapper} = setup({telegrafPlugins: [cpuPlugin, influxDB2Plugin]})

      expect(wrapper.exists()).toBe(true)
      expect(wrapper).toMatchSnapshot()
    })
  })
})
