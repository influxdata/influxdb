import React from 'react'
import {shallow} from 'enzyme'

import {IFQLPage} from 'src/ifql/containers/IFQLPage'
import TimeMachine from 'src/ifql/components/TimeMachine'

const setup = () => {
  const props = {
    links: {
      self: '',
      suggestions: '',
    },
  }

  const wrapper = shallow(<IFQLPage {...props} />)

  return {
    wrapper,
  }
}

describe('IFQL.Containers.IFQLPage', () => {
  describe('rendering', () => {
    it('renders the page', () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
    })

    it('renders the <TimeMachine/>', () => {
      const {wrapper} = setup()

      const timeMachine = wrapper.find(TimeMachine)

      expect(timeMachine.exists()).toBe(true)
    })
  })
})
