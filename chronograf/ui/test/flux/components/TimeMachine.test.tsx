import React from 'react'
import {shallow} from 'enzyme'
import TimeMachine from 'src/flux/components/TimeMachine'
import {source} from 'test/resources/v2'

const setup = () => {
  const props = {
    script: '',
    body: [],
    data: [],
    source,
    suggestions: [],
    onSubmitScript: () => {},
    onChangeScript: () => {},
    onValidate: () => {},
    onAppendFrom: () => {},
    onAppendJoin: () => {},
    onDeleteBody: () => {},
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
