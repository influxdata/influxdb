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
import {TIME_FIELD_DEFAULT} from 'src/shared/constants/tableGraph'

const defaultProps = {
  dataLabels: [],
  handleUpdateTableOptions: () => {},
  onResetFocus: () => {},
  tableOptions: {
    columnNames: [],
    fieldNames: [],
    fixFirstColumn: true,
    sortBy: {displayName: '', internalName: '', visible: true},
    timeFormat: '',
    verticalTimeAxis: true,
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
  describe('getters', () => {
    describe('fieldNames', () => {
      describe('if fieldNames are passed in tableOptions as props', () => {
        it('returns fieldNames', () => {
          const fieldNames = [
            {internalName: 'time', displayName: 'TIME', visible: true},
            {internalName: 'foo', displayName: 'BAR', visible: false},
          ]
          const {instance} = setup({tableOptions: {fieldNames}})

          expect(instance.fieldNames).toBe(fieldNames)
        })
      })

      describe('if fieldNames are not passed in tableOptions as props', () => {
        it('returns empty array', () => {
          const {instance} = setup()

          expect(instance.fieldNames).toEqual([])
        })
      })
    })

    describe('timeField', () => {
      describe('if time field in fieldNames', () => {
        it('returns time field', () => {
          const timeField = {
            internalName: 'time',
            displayName: 'TIME',
            visible: true,
          }
          const fieldNames = [
            timeField,
            {internalName: 'foo', displayName: 'BAR', visible: false},
          ]
          const {instance} = setup({tableOptions: {fieldNames}})

          expect(instance.timeField).toBe(timeField)
        })
      })

      describe('if time field not in fieldNames', () => {
        it('returns default time field', () => {
          const fieldNames = [
            {internalName: 'foo', displayName: 'BAR', visible: false},
          ]
          const {instance} = setup({tableOptions: {fieldNames}})

          expect(instance.timeField).toBe(TIME_FIELD_DEFAULT)
        })
      })
    })

    describe('computedFieldNames', () => {
      describe('if dataLabels are not passed in', () => {
        it('returns an array of the time column', () => {
          const {instance} = setup()

          expect(instance.computedFieldNames).toEqual([TIME_FIELD_DEFAULT])
        })
      })

      describe('if dataLabels are passed in', () => {
        describe('if dataLabel has a matching fieldName', () => {
          it('returns array with the matching fieldName', () => {
            const fieldNames = [
              {internalName: 'foo', displayName: 'bar', visible: true},
            ]
            const dataLabels = ['foo']
            const {instance} = setup({tableOptions: {fieldNames}, dataLabels})

            expect(instance.computedFieldNames).toEqual(fieldNames)
          })
        })

        describe('if dataLabel does not have a matching fieldName', () => {
          it('returns array with a new fieldName created for it', () => {
            const fieldNames = [
              {internalName: 'time', displayName: 'bar', visible: true},
            ]
            const unmatchedLabel = 'foo'
            const dataLabels = ['time', unmatchedLabel]
            const {instance} = setup({tableOptions: {fieldNames}, dataLabels})

            expect(instance.computedFieldNames).toEqual([
              ...fieldNames,
              {internalName: unmatchedLabel, displayName: '', visible: true},
            ])
          })
        })
      })
    })
  })

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
