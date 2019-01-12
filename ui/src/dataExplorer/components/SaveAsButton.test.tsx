// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import SaveAsButton from 'src/dataExplorer/components/SaveAsButton'
import SaveAsCellForm from 'src/dataExplorer/components/SaveAsCellForm'

const setup = () => {
  const wrapper = shallow(<SaveAsButton />)

  return {wrapper}
}

describe('SaveAsButton', () => {
  const {wrapper} = setup()
  describe('rendering', () => {
    it('renders', () => {
      expect(wrapper.exists()).toBe(true)
      expect(wrapper).toMatchSnapshot()
    })
  })

  describe('save as cell form', () => {
    it('defaults to save as cell form', () => {
      const saveAsCellForm = wrapper.find(SaveAsCellForm)

      expect(saveAsCellForm.exists()).toBe(true)
    })
  })
})
