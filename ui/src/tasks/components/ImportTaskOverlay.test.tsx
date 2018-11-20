// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import ImportTaskOverlay from 'src/tasks/components/ImportTaskOverlay'

const setup = (override?) => {
  const props = {
    onDismissOverlay: oneTestFunction,
    onSave: oneTestFunction,
    ...override,
  }

  const wrapper = shallow(<ImportTaskOverlay {...props} />)

  return {wrapper}
}
const oneTestFunction = jest.fn((script?: string, fileName?: string) => {
  script = fileName
  fileName = script
  return
})

describe('ImportTaskOverlay', () => {
  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
      expect(wrapper).toMatchSnapshot()
    })
  })
})
