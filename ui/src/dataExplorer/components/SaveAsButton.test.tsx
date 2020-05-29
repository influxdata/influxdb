// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import SaveAsButton from 'src/dataExplorer/components/SaveAsButton'

const setup = () => {
  const wrapper = shallow(<SaveAsButton />)

  return {wrapper}
}

describe('SaveAsButton', () => {
  const {wrapper} = setup()
  describe('rendering', () => {
    it('renders', () => {
      expect(wrapper.exists()).toBe(true)
    })
  })
})
