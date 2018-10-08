import React from 'react'
import FluxEditor from 'src/flux/components/TimeMachineEditor'
import {shallow} from 'enzyme'

const setup = (override?) => {
  const props = {
    script: '',
    onChangeScript: () => {},
    ...override,
  }

  const wrapper = shallow(<FluxEditor {...props} />)

  return {
    wrapper,
    props,
  }
}

describe('Flux.Components.FluxEditor', () => {
  describe('rendering', () => {
    it('renders without error', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })
  })
})
