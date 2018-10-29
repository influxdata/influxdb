import React from 'react'
import {shallow} from 'enzyme'

import MultiSelectDropdown from 'src/clockface/components/dropdowns/MultiSelectDropdown'
import DropdownButton from 'src/clockface/components/dropdowns/DropdownButton'

describe('MultiSelectDropdown', () => {
  let wrapper

  const wrapperSetup = (override = {}) => {
    const items = [
      {
        id: 'item-a',
        text: 'A',
      },
      {
        id: 'item-b',
        text: 'B',
      },
      {
        id: 'item-c',
        text: 'C',
      },
    ]

    const selectedIDs = [items[0].id]

    const children = items.map(item => (
      <MultiSelectDropdown.Item id={item.id} key={item.id} value={item}>
        {item.text}
      </MultiSelectDropdown.Item>
    ))

    const props = {
      children,
      selectedIDs,
      onChange: () => {},
      ...override,
    }

    return shallow(<MultiSelectDropdown {...props} />)
  }

  beforeEach(() => {
    jest.resetAllMocks()
    wrapper = wrapperSetup()
  })

  it('mounts without exploding', () => {
    expect(wrapper).toHaveLength(1)
  })

  it('matches snapshot with minimal props', () => {
    expect(wrapper).toMatchSnapshot()
  })

  describe('with menu expanded', () => {
    it('renders a dropdown menu', () => {
      const button = wrapper.find(DropdownButton)

      button.simulate('click')

      expect(wrapper.find('[data-test="dropdown-menu"]')).toHaveLength(1)
    })

    it('matches snapshot', () => {
      const button = wrapper.find(DropdownButton)

      button.simulate('click')

      expect(wrapper).toMatchSnapshot()
    })
  })
})
