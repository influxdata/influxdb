import React from 'react'
import TimeMachineEditor from 'src/flux/components/TimeMachineEditor'
import {shallow} from 'enzyme'

const setup = (override?) => {
  const props = {
    script: '',
    onChangeScript: () => {},
    ...override,
  }

  const wrapper = shallow(<TimeMachineEditor {...props} />)

  return {
    wrapper,
    props,
  }
}

describe('Flux.Components.TimeMachineEditor', () => {
  describe('rendering', () => {
    it('renders without error', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })
  })
})
