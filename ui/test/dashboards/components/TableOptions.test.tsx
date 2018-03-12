import React from 'react'

import {TableOptions} from 'src/dashboards/components/TableOptions'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import GraphOptionsTimeFormat from 'src/dashboards/components/GraphOptionsTimeFormat'
import GraphOptionsTimeAxis from 'src/dashboards/components/GraphOptionsTimeAxis'
import GraphOptionsSortBy from 'src/dashboards/components/GraphOptionsSortBy'
import GraphOptionsTextWrapping from 'src/dashboards/components/GraphOptionsTextWrapping'
import GraphOptionsCustomizeColumns from 'src/dashboards/components/GraphOptionsCustomizeColumns'
import GraphOptionsThresholds from 'src/dashboards/components/GraphOptionsThresholds'
import GraphOptionsThresholdColoring from 'src/dashboards/components/GraphOptionsThresholdColoring'

import {shallow} from 'enzyme'

const setup = (override = {}) => {
  const props = {
    singleStatType: '',
    singleStatColors: [],
    handleUpdateSingleStatType: () => {},
    handleUpdateSingleStatColors: () => {},
    handleUpdateOptions: () => {},
    options: {
      timeFormat: '',
      verticalTimeAxis: false,
      sortBy: {internalName: '', displayName: ''},
      wrapping: '',
      columnNames: [],
    },
    ...override,
  }

  const wrapper = shallow(<TableOptions {...props} />)

  return {wrapper, props}
}

describe('Dashboards.Components.TableOptions', () => {
  describe('rendering', () => {
    it('should render all components', () => {
      const {wrapper} = setup()
      const fancyScrollbar = wrapper.find(FancyScrollbar)
      const graphOptionsTimeFormat = wrapper.find(GraphOptionsTimeFormat)
      const graphOptionsTimeAxis = wrapper.find(GraphOptionsTimeAxis)
      const graphOptionsSortBy = wrapper.find(GraphOptionsSortBy)
      const graphOptionsTextWrapping = wrapper.find(GraphOptionsTextWrapping)
      const graphOptionsCustomizeColumns = wrapper.find(
        GraphOptionsCustomizeColumns
      )
      const graphOptionsThresholds = wrapper.find(GraphOptionsThresholds)
      const graphOptionsThresholdColoring = wrapper.find(
        GraphOptionsThresholdColoring
      )

      expect(fancyScrollbar.exists()).toBe(true)
      expect(graphOptionsTimeFormat.exists()).toBe(true)
      expect(graphOptionsTimeAxis.exists()).toBe(true)
      expect(graphOptionsSortBy.exists()).toBe(true)
      expect(graphOptionsTextWrapping.exists()).toBe(true)
      expect(graphOptionsCustomizeColumns.exists()).toBe(true)
      expect(graphOptionsThresholds.exists()).toBe(true)
      expect(graphOptionsThresholdColoring.exists()).toBe(true)
    })
  })
})
