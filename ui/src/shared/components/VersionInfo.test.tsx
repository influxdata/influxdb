// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import VersionInfo from 'src/shared/components/VersionInfo'

const setup = (override = {}) => {
  const props = {
    ...override,
  }

  const wrapper = shallow(<VersionInfo {...props} />)

  return {wrapper}
}

describe('VersionInfo', () => {
  it('renders correctly', () => {
    const {wrapper} = setup()

    expect(wrapper.exists()).toBe(true)
  })

  describe('when width is specified', () => {
    it('renders corectly', () => {
      const {wrapper} = setup({widthPixels: 300})

      expect(wrapper.exists()).toBe(true)
    })
  })
})
