import ThresholdProps from 'src/dashboards/components/Threshold'

import React from 'react'
import {shallow} from 'enzyme'

describe('Threshold', () => {
  it('renders without an error', () => {
    const props = {
      visualizationType: 'gauge',
      threshold: {
        type: 'color',
        hex: '#444444',
        id: 'id',
        name: 'name',
        value: '2',
      },
      disableMaxColor: true,
      onChooseColor: () => {},
      onValidateColorValue: () => true,
      onUpdateColorValue: () => {},
      onDeleteThreshold: () => {},
      isMin: false,
      isMax: true,
    }

    const wrapper = shallow(<ThresholdProps {...props} />)
    expect(wrapper.exists()).toBe(true)
  })
})
