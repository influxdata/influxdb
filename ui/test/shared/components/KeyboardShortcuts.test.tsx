import React from 'react'
import {shallow} from 'enzyme'
import KeyboardShortcuts from 'src/shared/components/KeyboardShortcuts'

const map = {
  keydown: null,
}

const setup = (override?) => {
  const props = {
    onControlEnter: () => {},
    children: <div />,
    ...override,
  }

  document.addEventListener = jest.fn((event, cb) => {
    map[event] = cb
  })

  const wrapper = shallow(<KeyboardShortcuts {...props} />)

  return {
    wrapper,
  }
}

describe('Shared.Comonents.KeyboardShortcuts', () => {
  describe('rendering', () => {
    it('renders without errors', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })
  })

  describe('user interraction', () => {
    describe('control + shift', () => {
      const onControlEnter = jest.fn()
      setup({onControlEnter})

      const event = {ctrlKey: true, key: 'Enter', preventDefault: () => {}}
      map.keydown(event)

      expect(onControlEnter).toHaveBeenCalledTimes(1)
    })
  })
})
