import React from 'react'
import {shallow} from 'enzyme'
import FuncArgInput from 'src/flux/components/FuncArgInput'

const setup = (override?) => {
  const props = {
    funcID: '1',
    argKey: 'db',
    value: 'db1',
    type: 'string',
    onChangeArg: () => {},
    onGenerateScript: () => {},
    ...override,
  }

  const wrapper = shallow(<FuncArgInput {...props} />)

  return {
    wrapper,
    props,
  }
}

describe('Flux.Components.FuncArgInput', () => {
  describe('rendering', () => {
    it('renders without errors', () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
    })
  })

  describe('user interraction', () => {
    describe('typing', () => {
      describe('hitting enter', () => {
        it('generates a new script when Enter is pressed', () => {
          const onGenerateScript = jest.fn()
          const preventDefault = jest.fn()

          const {wrapper} = setup({onGenerateScript})

          const input = wrapper.find('input')
          input.simulate('keydown', {key: 'Enter', preventDefault})

          expect(onGenerateScript).toHaveBeenCalledTimes(1)
          expect(preventDefault).toHaveBeenCalledTimes(1)
        })

        it('it does not generate a new script when typing', () => {
          const onGenerateScript = jest.fn()
          const preventDefault = jest.fn()

          const {wrapper} = setup({onGenerateScript})

          const input = wrapper.find('input')
          input.simulate('keydown', {key: 'a', preventDefault})

          expect(onGenerateScript).not.toHaveBeenCalled()
          expect(preventDefault).not.toHaveBeenCalled()
        })
      })

      describe('changing the input value', () => {
        it('calls onChangeArg', () => {
          const onChangeArg = jest.fn()
          const {wrapper, props} = setup({onChangeArg})

          const input = wrapper.find('input')
          const value = 'db2'
          input.simulate('change', {target: {value}})
          const {funcID, argKey} = props

          expect(onChangeArg).toHaveBeenCalledWith({funcID, key: argKey, value})
        })
      })
    })
  })
})
