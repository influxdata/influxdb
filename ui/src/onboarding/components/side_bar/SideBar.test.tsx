// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import SideBar from 'src/onboarding/components/side_bar/SideBar'

// Types
import {SideBarTabStatus as TabStatus} from 'src/onboarding/components/side_bar/SideBar'

// Constants
import {ComponentColor} from 'src/clockface'
import {IconFont} from 'src/clockface'

const onClick = jest.fn(() => {})

const childrenArray = [
  <SideBar.Tab
    label={'a'}
    key={'a'}
    id={'a'}
    active={true}
    status={TabStatus.Default}
    onClick={onClick}
  />,
  <SideBar.Tab
    label={'b'}
    key={'b'}
    id={'b'}
    active={false}
    status={TabStatus.Default}
    onClick={onClick}
  />,
  <SideBar.Button
    key="a"
    text="a"
    titleText="a"
    color={ComponentColor.Secondary}
    icon={IconFont.Download}
  />,
  <SideBar.Button
    text="b"
    key="b"
    titleText="b"
    color={ComponentColor.Default}
    icon={IconFont.Plus}
  />,
]

const setup = (override?, childrenArray = []) => {
  const props = {
    title: 'titleString',
    visible: true,
    ...override,
  }

  const wrapper = shallow(<SideBar {...props}>{childrenArray} </SideBar>)

  return {wrapper}
}

describe('SideBar', () => {
  describe('rendering', () => {
    it('renders with no children', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
      expect(wrapper).toMatchSnapshot()
    }),
      it('renders with children, and renders its children', () => {
        const {wrapper} = setup(null, childrenArray)
        expect(wrapper.exists()).toBe(true)
        expect(wrapper.contains(childrenArray[0])).toBe(true)
        expect(wrapper.find(SideBar.Tab)).toHaveLength(2)
        expect(wrapper.find(SideBar.Button)).toHaveLength(2)
        expect(wrapper).toMatchSnapshot()
      })
  })
})
