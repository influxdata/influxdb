import React from 'react'
import {shallow} from 'enzyme'

import IFQLPage from 'src/ifql/containers/IFQLPage'

const setup = () => {
  const wrapper = shallow(<IFQLPage />)

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
  })
})
