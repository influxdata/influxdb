// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import SideBarTab from 'src/dataLoaders/components/side_bar/SideBarTab'

// Types
import {SideBarTabStatus as TabStatus} from 'src/dataLoaders/components/side_bar/SideBar'

// Constants
import {IconFont} from 'src/clockface'

const onClick = jest.fn(() => {})

const setup = (override?) => {
  const props = {
    label: 'label',
    key: 'key',
    id: 'id',
    active: true,
    status: TabStatus.Default,
    onClick,
    ...override,
  }

  const wrapper = shallow(<SideBarTab {...props} />)

  return {wrapper}
}

describe('SideBarTab', () => {
  describe('rendering', () => {
    it('renders! wee!', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })

    it('renders a checkmark if status success', () => {
      const {wrapper} = setup({status: TabStatus.Success})
      expect(wrapper.exists()).toBe(true)
      expect(wrapper.find(`.${IconFont.Checkmark}`)).toHaveLength(1)
    })
  })
})
