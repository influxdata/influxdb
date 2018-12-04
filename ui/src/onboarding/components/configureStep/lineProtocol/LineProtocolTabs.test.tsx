// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {LineProtocolTabs} from 'src/onboarding/components/configureStep/lineProtocol/LineProtocolTabs'

import {LineProtocolTab} from 'src/types/v2/dataLoaders'

const setup = (override?) => {
  const props = {
    tabs: [
      LineProtocolTab.UploadFile,
      LineProtocolTab.EnterManually,
      LineProtocolTab.EnterURL,
    ],
    ...override,
  }

  const wrapper = shallow(<LineProtocolTabs {...props} />)

  return {wrapper}
}

describe('LineProtocolTabs', () => {
  describe('rendering', () => {
    it('renders!', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)

      expect(wrapper).toMatchSnapshot()
    })
  })
})
