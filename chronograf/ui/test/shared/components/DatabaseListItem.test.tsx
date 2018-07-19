import React from 'react'

import {shallow} from 'enzyme'

import DatabaseListItem from 'src/shared/components/DatabaseListItem'

const namespace = {database: 'db1', retentionPolicy: 'rp1'}

const setup = (override = {}) => {
  const props = {
    isActive: false,
    namespace,
    onChooseNamespace: () => () => {},
    ...override,
  }

  const item = shallow(<DatabaseListItem {...props} />)
  return {
    item,
    props,
  }
}

describe('Shared.Components.DatabaseListItem', () => {
  describe('rendering', () => {
    it('renders the <DatabaseListItem />', () => {
      const {item} = setup()

      expect(item.exists()).toBe(true)
    })

    describe('if isActive is false', () => {
      it('does not have the `.active` class', () => {
        const {item} = setup()

        expect(item.hasClass('active')).toBe(false)
      })

      it('does have the `.active` class', () => {
        const {item} = setup({isActive: true})

        expect(item.hasClass('active')).toBe(true)
      })
    })
  })

  describe('callbacks', () => {
    describe('onChooseNamespace', () => {
      it('calls onChooseNamespace with the items namespace when clicked', () => {
        const onChooseNamespace = jest.fn()
        const {item} = setup({onChooseNamespace})

        item.simulate('click')

        expect(onChooseNamespace).toHaveBeenCalledTimes(1)
        expect(onChooseNamespace).toBeCalledWith(namespace)
      })
    })
  })
})
