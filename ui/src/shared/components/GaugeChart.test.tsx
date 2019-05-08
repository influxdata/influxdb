import {shallow} from 'enzyme'
import React from 'react'
import Gauge from 'src/shared/components/Gauge'
import GaugeChart from 'src/shared/components/GaugeChart'
import {ViewType, ViewShape, GaugeView} from 'src/types/dashboards'

describe('GaugeChart', () => {
  describe('render', () => {
    describe('when data has a value', () => {
      it('renders the correct number', () => {
        const props = {
          value: 2,
          properties: {
            queries: [],
            colors: [],
            shape: ViewShape.ChronografV2,
            type: ViewType.Gauge,
            prefix: '',
            suffix: '',
            note: '',
            showNoteWhenEmpty: false,
            decimalPlaces: {
              digits: 10,
              isEnforced: false,
            },
          } as GaugeView,
        }

        const wrapper = shallow(<GaugeChart {...props} />)

        expect(wrapper.find(Gauge).exists()).toBe(true)
        expect(wrapper.find(Gauge).props().gaugePosition).toBe(2)
      })
    })
  })
})
