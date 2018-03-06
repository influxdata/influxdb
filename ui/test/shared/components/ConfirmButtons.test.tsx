import React from 'react'
import ConfirmButtons from 'src/shared/components/ConfirmButtons'
import ConfirmButton from 'src/shared/components/ConfirmButton'

import {shallow} from 'enzyme'

const props = {}

describe('Componenets.Shared.ConfirmButtons', () => {
  describe('rendering', () => {
    it('has a confirm and cancel button', () => {
      const wrapper = shallow(<ConfirmButtons {...props} />)
      const buttons = wrapper.dive().find(ConfirmButton)
      expect(buttons.length).toBe(2)
    })
  })

  describe('user interaction', () => {
    it('')
  })
})
