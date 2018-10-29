import {shallow} from 'enzyme'
import React from 'react'
import Gauge from 'src/shared/components/Gauge'
import GaugeChart from 'src/shared/components/GaugeChart'
import {ViewType, ViewShape, GaugeView} from 'src/types/v2/dashboards'

const tables = [
  {
    id: '54797afd-734d-4ca3-94b6-3a7870c53b27',
    data: [
      ['', 'result', 'table', '_time', 'mean', '_measurement'],
      ['', '', '0', '2018-09-27T16:50:10Z', '2', 'cpu'],
    ],
    name: '_measurement=cpu',
    groupKey: {
      _measurement: 'cpu',
    },
    dataTypes: {
      '': '#datatype',
      result: 'string',
      table: 'long',
      _time: 'dateTime:RFC3339',
      mean: 'double',
      _measurement: 'string',
    },
  },
]

const properties: GaugeView = {
  queries: [],
  colors: [],
  shape: ViewShape.ChronografV2,
  type: ViewType.Gauge,
  prefix: '',
  suffix: '',
  decimalPlaces: {
    digits: 10,
    isEnforced: false,
  },
}

const defaultProps = {
  tables: [],
  properties,
}

const setup = (overrides = {}) => {
  const props = {
    ...defaultProps,
    ...overrides,
  }

  return shallow(<GaugeChart {...props} />)
}

describe('GaugeChart', () => {
  describe('render', () => {
    describe('when data is empty', () => {
      it('renders the correct number', () => {
        const wrapper = setup()

        expect(wrapper.find(Gauge).exists()).toBe(true)
        expect(wrapper.find(Gauge).props().gaugePosition).toBe(0)
      })
    })

    describe('when data has a value', () => {
      it('renders the correct number', () => {
        const wrapper = setup({tables})

        expect(wrapper.find(Gauge).exists()).toBe(true)
        expect(wrapper.find(Gauge).props().gaugePosition).toBe('2')
      })
    })
  })
})
