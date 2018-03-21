import React from 'react'

import {TableOptions} from 'src/dashboards/components/TableOptions'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import GraphOptionsTimeFormat from 'src/dashboards/components/GraphOptionsTimeFormat'
import GraphOptionsTimeAxis from 'src/dashboards/components/GraphOptionsTimeAxis'
import GraphOptionsSortBy from 'src/dashboards/components/GraphOptionsSortBy'
import GraphOptionsTextWrapping from 'src/dashboards/components/GraphOptionsTextWrapping'
import GraphOptionsCustomizeColumns from 'src/dashboards/components/GraphOptionsCustomizeColumns'
import ThresholdsList from 'src/shared/components/ThresholdsList'
import ThresholdsListTypeToggle from 'src/shared/components/ThresholdsListTypeToggle'
import GraphOptionsFixFirstColumn from 'src/dashboards/components/GraphOptionsFixFirstColumn'

import {shallow} from 'enzyme'

const queryConfigs = [
  {
    measurement: 'dev',
    fields: [
      {
        alias: 'boom',
        value: 'test',
      },
      {
        alias: 'again',
        value: 'again',
      },
    ],
  },
  {
    measurement: 'prod',
    fields: [
      {
        alias: 'boom',
        value: 'test',
      },
      {
        alias: 'again',
        value: 'again',
      },
    ],
  },
]

const defaultProps = {
  queryConfigs: queryConfigs,
  handleUpdateTableOptions: () => {},
  tableOptions: {
    timeFormat: '',
    verticalTimeAxis: true,
    sortBy: {internalName: '', displayName: ''},
    wrapping: '',
    columnNames: [],
    fixFirstColumn: true,
  },
  onResetFocus: () => {},
}

const setup = (override = {}) => {
  const props = {
    ...defaultProps,
    ...override,
  }

  const wrapper = shallow(<TableOptions {...props} />)

  return {wrapper, props}
}

describe('Dashboards.Components.TableOptions', () => {
  describe('getters', () => {
    describe('computedColumnNames', () => {
      it('returns the correct column names', () => {
        const instance = new TableOptions(defaultProps)

        const expected = [
          {
            displayName: '',
            internalName: 'time',
          },
          {
            displayName: '',
            internalName: 'dev.boom',
          },
          {
            displayName: '',
            internalName: 'dev.again',
          },
          {
            displayName: '',
            internalName: 'prod.boom',
          },
          {
            displayName: '',
            internalName: 'prod.again',
          },
        ]

        expect(instance.computedColumnNames).toEqual(expected)
      })
    })
  })

  describe('rendering', () => {
    it('should render all components', () => {
      const queryConfigs = [
        {
          measurement: 'dev',
          fields: [
            {
              alias: 'boom',
              value: 'test',
            },
          ],
        },
      ]

      const expectedSortOptions = [
        {
          key: 'time',
          text: 'time',
        },
        {
          key: 'dev.boom',
          text: 'dev.boom',
        },
      ]

      const {wrapper} = setup({queryConfigs})
      const fancyScrollbar = wrapper.find(FancyScrollbar)
      const graphOptionsTimeFormat = wrapper.find(GraphOptionsTimeFormat)
      const graphOptionsTimeAxis = wrapper.find(GraphOptionsTimeAxis)
      const graphOptionsSortBy = wrapper.find(GraphOptionsSortBy)
      const graphOptionsTextWrapping = wrapper.find(GraphOptionsTextWrapping)
      const graphOptionsFixFirstColumn = wrapper.find(
        GraphOptionsFixFirstColumn
      )
      const graphOptionsCustomizeColumns = wrapper.find(
        GraphOptionsCustomizeColumns
      )
      const thresholdsList = wrapper.find(ThresholdsList)
      const thresholdsListTypeToggle = wrapper.find(ThresholdsListTypeToggle)

      expect(graphOptionsSortBy.props().sortByOptions).toEqual(
        expectedSortOptions
      )

      expect(fancyScrollbar.exists()).toBe(true)
      expect(graphOptionsTimeFormat.exists()).toBe(true)
      expect(graphOptionsTimeAxis.exists()).toBe(true)
      expect(graphOptionsSortBy.exists()).toBe(true)
      expect(graphOptionsTextWrapping.exists()).toBe(true)
      expect(graphOptionsFixFirstColumn.exists()).toBe(true)
      expect(graphOptionsCustomizeColumns.exists()).toBe(true)
      expect(thresholdsList.exists()).toBe(true)
      expect(thresholdsListTypeToggle.exists()).toBe(true)
    })
  })
})
