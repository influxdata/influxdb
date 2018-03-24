import React from 'react'
import {shallow} from 'enzyme'
import FuncsButton from 'src/ifql/components/FuncsButton'

const setup = (override = {}) => {
  const props = {
    funcs: ['f1', 'f2'],
    ...override,
  }

  const wrapper = shallow(<FuncsButton {...props} />)

  return {
    wrapper,
  }
}

describe('IFQL.Components.FuncsButton', () => {
  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
    })

    describe('the function list', () => {
      it('does not render the list of funcs by default', () => {
        const {wrapper} = setup()

        const list = wrapper.find({'data-test': 'func-li'})

        expect(list.exists()).toBe(false)
      })
    })
  })

  describe('user interraction', () => {
    describe('clicking the add function button', () => {
      it('displays the list of functions', () => {
        const {wrapper} = setup()

        wrapper.simulate('click')

        const list = wrapper.find({'data-test': 'func-item'})

        expect(list.length).toBe(2)
        expect(list.first().text()).toBe('f1')
        expect(list.last().text()).toBe('f2')
      })
    })
  })
})
