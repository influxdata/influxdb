import {shallow} from 'enzyme'
import React from 'react'
import MeasurementListItem from 'src/shared/components/MeasurementListItem'
import TagList from 'src/shared/components/TagList'

import {query as defaultQuery} from 'mocks/dummy'

const setup = (overrides = {}) => {
  const props = {
    query: defaultQuery,
    querySource: {
      links: {
        proxy: '',
      },
    },
    isActive: true,
    measurement: defaultQuery.measurement,
    numTagsActive: 3,
    areTagsAccepted: true,
    isQuerySupportedByExplorer: true,
    onChooseTag: () => {},
    onGroupByTag: () => {},
    onAcceptReject: () => {},
    onChooseMeasurement: () => () => {},
    ...overrides,
  }

  return shallow(<MeasurementListItem {...props} />)
}

describe('MeasurementListItem', () => {
  describe('render', () => {
    describe('toggling measurement', () => {
      it('shows the correct state', () => {
        const trigger = jest.fn()
        const factory = jest.fn(() => trigger)
        const wrapper = setup({onChooseMeasurement: factory})

        expect(wrapper.find(TagList).exists()).toBe(true)

        wrapper.simulate('click')
        expect(factory).not.toHaveBeenCalled()

        expect(wrapper.find(TagList).exists()).toBe(false)

        wrapper.simulate('click')
        expect(factory).not.toHaveBeenCalled()

        expect(wrapper.find(TagList).exists()).toBe(true)
      })
    })

    it('triggers callback when not current measurement', () => {
      const trigger = jest.fn()
      let measurement = 'boo'
      const query = {...defaultQuery, measurement}

      const factory = jest.fn((m: string) => {
        measurement = m
        return trigger
      })

      const wrapper = setup({
        onChooseMeasurement: factory,
        query,
      })

      expect(wrapper.find(TagList).exists()).toBe(false)

      wrapper.simulate('click')
      wrapper.setProps({query: {...defaultQuery, measurement}})
      expect(factory).toHaveBeenCalledWith(defaultQuery.measurement)
      expect(trigger).toHaveBeenCalled()

      expect(wrapper.find(TagList).exists()).toBe(true)
    })
  })
})
