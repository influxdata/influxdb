// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import SortingHat from 'src/shared/components/sorting_hat/SortingHat'

// Types
import {Sort} from 'src/clockface/types'

const users = [
  {user: {name: 'fred'}, age: 48},
  {user: {name: 'barney'}, age: 34},
  {user: {name: 'fred'}, age: 40},
  {user: {name: 'barney'}, age: 36},
]

const setup = (override?) => {
  const children = jest.fn(() => <div />)
  const props = {
    list: users,
    sortKey: 'user.name',
    direction: Sort.Ascending,
    children,
    ...override,
  }

  const wrapper = shallow(<SortingHat {...props} />)

  return {wrapper, children}
}

describe('SortingHat', () => {
  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })
  })

  describe('sorting', () => {
    it('can sort a nested object by sort key', () => {
      const sortKey = 'user.name'
      const direction = Sort.Ascending
      const expected = [
        {user: {name: 'barney'}, age: 34},
        {user: {name: 'barney'}, age: 36},
        {user: {name: 'fred'}, age: 48},
        {user: {name: 'fred'}, age: 40},
      ]

      const {children} = setup({sortKey, direction})

      expect(children).toBeCalledWith(expected)
    })
  })
})
