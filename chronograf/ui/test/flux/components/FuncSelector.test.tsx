import React from 'react'
import {shallow} from 'enzyme'
import {FuncSelector} from 'src/flux/components/FuncSelector'
import FuncSelectorInput from 'src/shared/components/FuncSelectorInput'
import FuncListItem from 'src/flux/components/FuncListItem'
import FuncList from 'src/flux/components/FuncList'

const setup = (override = {}) => {
  const props = {
    funcs: ['count', 'range'],
    bodyID: '1',
    declarationID: '2',
    onAddNode: () => {},
    ...override,
  }

  const wrapper = shallow(<FuncSelector {...props} />)

  return {
    props,
    wrapper,
  }
}

describe('Flux.Components.FuncsButton', () => {
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
        const {wrapper, props} = setup()
        const [func1, func2] = props.funcs

        const dropdownButton = wrapper.find('button')
        dropdownButton.simulate('click')

        const list = wrapper
          .find(FuncList)
          .dive()
          .find(FuncListItem)

        const first = list.first().dive()
        const last = list.last().dive()

        expect(list.length).toBe(2)
        expect(first.text()).toBe(func1)
        expect(last.text()).toBe(func2)
      })
    })

    describe('filtering the list', () => {
      it('displays the filtered funcs', () => {
        const {wrapper, props} = setup()
        const [func1, func2] = props.funcs

        const dropdownButton = wrapper.find('button')
        dropdownButton.simulate('click')

        let list = wrapper
          .find(FuncList)
          .dive()
          .find(FuncListItem)

        const first = list.first().dive()
        const last = list.last().dive()

        expect(list.length).toBe(2)
        expect(first.text()).toBe(func1)
        expect(last.text()).toBe(func2)

        const input = wrapper
          .find(FuncList)
          .dive()
          .find(FuncSelectorInput)
          .dive()
          .find('input')

        input.simulate('change', {target: {value: 'ra'}})
        wrapper.update()

        list = wrapper
          .find(FuncList)
          .dive()
          .find(FuncListItem)

        const func = list.first()

        expect(list.length).toBe(1)
        expect(func.dive().text()).toBe(func2)
      })
    })

    describe('exiting the list', () => {
      it('closes when ESC is pressed', () => {
        const {wrapper} = setup()

        const dropdownButton = wrapper.find('button')
        dropdownButton.simulate('click')

        let list = wrapper.find(FuncList).dive()
        const input = list
          .find(FuncSelectorInput)
          .dive()
          .find('input')

        input.simulate('keyDown', {key: 'Escape'})
        wrapper.update()

        list = wrapper.find(FuncList)

        expect(list.exists()).toBe(false)
      })
    })

    describe('selecting a function with the keyboard', () => {
      describe('ArrowDown', () => {
        it('adds a function to the page', () => {
          const onAddNode = jest.fn()
          const {wrapper, props} = setup({onAddNode})
          const [, func2] = props.funcs
          const {bodyID, declarationID} = props

          const dropdownButton = wrapper.find('button')
          dropdownButton.simulate('click')

          const list = wrapper.find(FuncList).dive()

          const input = list
            .find(FuncSelectorInput)
            .dive()
            .find('input')

          input.simulate('keyDown', {key: 'ArrowDown'})
          input.simulate('keyDown', {key: 'Enter'})

          expect(onAddNode).toHaveBeenCalledWith(func2, bodyID, declarationID)
        })
      })
    })
  })
})
