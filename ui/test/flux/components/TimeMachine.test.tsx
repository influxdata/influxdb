import React from 'react'
import {shallow} from 'enzyme'
import TimeMachine from 'src/flux/components/TimeMachine'
import {service} from 'test/resources'

const setup = () => {
  const props = {
    script: '',
    body: [],
    data: [],
    service,
    suggestions: [],
    onSubmitScript: () => {},
    onChangeScript: () => {},
    onAnalyze: () => {},
    onAppendFrom: () => {},
    onAppendJoin: () => {},
    status: {type: '', text: ''},
  }

  const wrapper = shallow(<TimeMachine {...props} />)

  return {
    wrapper,
  }
}

describe('Flux.Components.TimeMachine', () => {
  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
    })
  })
})
