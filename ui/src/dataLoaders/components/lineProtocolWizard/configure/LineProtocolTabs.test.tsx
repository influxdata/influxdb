// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {LineProtocolTabs} from 'src/dataLoaders/components/lineProtocolWizard/configure/LineProtocolTabs'

import {LineProtocolTab} from 'src/types/dataLoaders'

const setup = (override?) => {
  const props = {
    tabs: [
      LineProtocolTab.UploadFile,
      LineProtocolTab.EnterManually,
      LineProtocolTab.EnterURL,
    ],
    bucket: 'a',
    org: 'a',
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
    })
  })
})
