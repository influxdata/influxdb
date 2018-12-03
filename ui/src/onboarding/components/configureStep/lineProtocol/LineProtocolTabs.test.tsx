// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import LineProtocolTabs from './LineProtocolTabs'

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
    }),
      it('selects first tab as activeTab on load', () => {
        const reorderedTabs = [
          LineProtocolTab.EnterManually,
          LineProtocolTab.UploadFile,
          LineProtocolTab.EnterURL,
        ]
        const {wrapper} = setup({tabs: reorderedTabs})
        expect(wrapper.state('activeTab')).toBe(reorderedTabs[0])
      })
  })
})
