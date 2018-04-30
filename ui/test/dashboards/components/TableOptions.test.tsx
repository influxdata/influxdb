import {shallow} from 'enzyme'
import React from 'react'
import GraphOptionsCustomizeFields from 'src/dashboards/components/GraphOptionsCustomizeFields'
import GraphOptionsFixFirstColumn from 'src/dashboards/components/GraphOptionsFixFirstColumn'
import GraphOptionsSortBy from 'src/dashboards/components/GraphOptionsSortBy'
import GraphOptionsTimeAxis from 'src/dashboards/components/GraphOptionsTimeAxis'
import GraphOptionsTimeFormat from 'src/dashboards/components/GraphOptionsTimeFormat'
import {TableOptions} from 'src/dashboards/components/TableOptions'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import ThresholdsList from 'src/shared/components/ThresholdsList'
import ThresholdsListTypeToggle from 'src/shared/components/ThresholdsListTypeToggle'

const defaultProps = {
  handleUpdateTableOptions: () => {},
  handleUpdateFieldOptions: () => {},
  handleChangeTimeFormat: () => {},
  handleChangeDecimalPlaces: () => {},
  onResetFocus: () => {},
  queryConfigs: [],
  tableOptions: {
    fixFirstColumn: true,
    sortBy: {displayName: '', internalName: '', visible: true},
    verticalTimeAxis: true,
  },
  fieldOptions: [],
  timeFormat: '',
  decimalPlaces: {
    isEnforced: true,
    digits: 2,
  },
}

const setup = (override = {}) => {
  const props = {
    ...defaultProps,
    ...override,
  }

  const wrapper = shallow(<TableOptions {...props} />)

  const instance = wrapper.instance() as TableOptions

  return {wrapper, instance, props}
}

describe('Dashboards.Components.TableOptions', () => {
  describe('rendering', () => {
    it('should render all components', () => {
      const {wrapper} = setup()
      const fancyScrollbar = wrapper.find(FancyScrollbar)
      const graphOptionsTimeFormat = wrapper.find(GraphOptionsTimeFormat)
      const graphOptionsTimeAxis = wrapper.find(GraphOptionsTimeAxis)
      const graphOptionsSortBy = wrapper.find(GraphOptionsSortBy)
      const graphOptionsFixFirstColumn = wrapper.find(
        GraphOptionsFixFirstColumn
      )
      const graphOptionsCustomizeFields = wrapper.find(
        GraphOptionsCustomizeFields
      )
      const thresholdsList = wrapper.find(ThresholdsList)
      const thresholdsListTypeToggle = wrapper.find(ThresholdsListTypeToggle)

      expect(fancyScrollbar.exists()).toBe(true)
      expect(graphOptionsTimeFormat.exists()).toBe(true)
      expect(graphOptionsTimeAxis.exists()).toBe(true)
      expect(graphOptionsSortBy.exists()).toBe(true)
      expect(graphOptionsFixFirstColumn.exists()).toBe(true)
      expect(graphOptionsCustomizeFields.exists()).toBe(true)
      expect(thresholdsList.exists()).toBe(true)
      expect(thresholdsListTypeToggle.exists()).toBe(true)
    })
  })
})
