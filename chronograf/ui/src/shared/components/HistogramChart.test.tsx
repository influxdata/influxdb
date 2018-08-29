import React from 'react'
import {mount} from 'enzyme'

import HistogramChart from 'src/shared/components/HistogramChart'
import HistogramChartTooltip from 'src/shared/components/HistogramChartTooltip'

import {RemoteDataState} from 'src/types'

describe('HistogramChart', () => {
  test('displays a HistogramChartSkeleton if empty data is passed', () => {
    const props = {
      data: [],
      dataStatus: RemoteDataState.Done,
      width: 600,
      height: 400,
      colorScale: () => 'blue',
      colors: [],
      sortBarGroups: (a, b) => a.value - b.value,
    }

    const wrapper = mount(<HistogramChart {...props} />)

    expect(wrapper).toMatchSnapshot()
  })

  test('displays a nothing if passed width and height of 0', () => {
    const props = {
      data: [],
      dataStatus: RemoteDataState.Done,
      width: 0,
      height: 0,
      colorScale: () => 'blue',
      colors: [],
      sortBarGroups: (a, b) => a.value - b.value,
    }

    const wrapper = mount(<HistogramChart {...props} />)

    expect(wrapper).toMatchSnapshot()
  })

  test('displays the visualization with bars if nonempty data is passed', () => {
    const props = {
      data: [
        {key: '0', time: 0, value: 0, group: 'a'},
        {key: '1', time: 1, value: 1, group: 'a'},
        {key: '2', time: 2, value: 2, group: 'b'},
      ],
      dataStatus: RemoteDataState.Done,
      width: 600,
      height: 400,
      colorScale: () => 'blue',
      colors: [],
      sortBarGroups: (a, b) => a.value - b.value,
    }

    const wrapper = mount(<HistogramChart {...props} />)

    expect(wrapper).toMatchSnapshot()
  })

  test('displays a HistogramChartTooltip when hovering over bars', () => {
    const props = {
      data: [
        {key: '0', time: 0, value: 0, group: 'a'},
        {key: '1', time: 1, value: 1, group: 'a'},
        {key: '2', time: 2, value: 2, group: 'b'},
      ],
      dataStatus: RemoteDataState.Done,
      width: 600,
      height: 400,
      colorScale: () => 'blue',
      colors: [],
      sortBarGroups: (a, b) => a.value - b.value,
    }

    const wrapper = mount(<HistogramChart {...props} />)

    const fakeMouseOverEvent = {
      target: {
        getBoundingClientRect() {
          return {top: 10, right: 10, bottom: 5, left: 5}
        },
      },
    }

    wrapper
      .find('.histogram-chart-bars--bars')
      .first()
      .simulate('mouseover', fakeMouseOverEvent)

    const tooltip = wrapper.find(HistogramChartTooltip)

    expect(tooltip).toMatchSnapshot()
  })
})
