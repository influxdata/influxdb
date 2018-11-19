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
    sortKeys: [],
    directions: [],
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
    it('can sort a nested object by multiple columns', () => {
      const sortKeys = ['user.name', 'age']
      const directions = [Sort.Ascending, Sort.Descending]
      const expected = [
        {user: {name: 'barney'}, age: 36},
        {user: {name: 'barney'}, age: 34},
        {user: {name: 'fred'}, age: 48},
        {user: {name: 'fred'}, age: 40},
      ]

      const {children} = setup({sortKeys, directions})

      expect(children).toBeCalledWith(expected)
    })
  })
})
