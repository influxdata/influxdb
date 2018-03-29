import React from 'react'

import {shallow} from 'enzyme'

import TableGraph from 'src/shared/components/TableGraph'
import {DEFAULT_SORT} from 'src/shared/constants/tableGraph'
import {
  filterTableColumns,
  processTableData,
} from 'src/utils/timeSeriesTransformers.js'

const setup = (override = []) => {
  const props = {
    data: [],
    tableOptions: {
      timeFormat: '',
      verticalTimeAxis: true,
      sortBy: {
        internalName: '',
        displayName: '',
        visible: true,
      },
      wrapping: '',
      fieldNames: [],
      fixFirstColumn: true,
    },
    hoverTime: '',
    onSetHoverTime: () => {},
    colors: [],
    setDataLabels: () => {},
    ...override,
  }

  const data = [
    ['time', 'f1', 'f2'],
    [1000, 3000, 2000],
    [2000, 1000, 3000],
    [3000, 2000, 1000],
  ]

  const wrapper = shallow(<TableGraph {...props} />)
  const instance = wrapper.instance() as TableGraph
  return {wrapper, instance, props, data}
}

describe('Components.Shared.TableGraph', () => {
  describe('functions', () => {
    describe('filterTableColumns', () => {
      it("returns a nested array of that only include columns whose corresponding fieldName's visibility is true", () => {
        const {data} = setup()

        const fieldNames = [
          {internalName: 'time', displayName: 'Time', visible: true},
          {internalName: 'f1', displayName: '', visible: false},
          {internalName: 'f2', displayName: 'F2', visible: false},
        ]

        const actual = filterTableColumns(data, fieldNames)
        const expected = [['time'], [1000], [2000], [3000]]
        expect(actual).toEqual(expected)
      })

      it('returns an array of an empty array if all fieldNames are not visible', () => {
        const {data} = setup()

        const fieldNames = [
          {internalName: 'time', displayName: 'Time', visible: false},
          {internalName: 'f1', displayName: '', visible: false},
          {internalName: 'f2', displayName: 'F2', visible: false},
        ]

        const actual = filterTableColumns(data, fieldNames)
        const expected = [[]]
        expect(actual).toEqual(expected)
      })
    })

    describe('processTableData', () => {
      it('sorts the data based on the provided sortFieldName', () => {
        const {data} = setup()
        const sortFieldName = 'f1'
        const direction = DEFAULT_SORT
        const verticalTimeAxis = true

        const fieldNames = [
          {internalName: 'time', displayName: 'Time', visible: true},
          {internalName: 'f1', displayName: '', visible: true},
          {internalName: 'f2', displayName: 'F2', visible: true},
        ]

        const actual = processTableData(
          data,
          sortFieldName,
          direction,
          verticalTimeAxis,
          fieldNames
        )
        const expected = [
          ['time', 'f1', 'f2'],
          [2000, 1000, 3000],
          [3000, 2000, 1000],
          [1000, 3000, 2000],
        ]

        expect(actual.processedData).toEqual(expected)
      })

      it('filters out invisible columns', () => {
        const {data} = setup()
        const sortFieldName = 'time'
        const direction = DEFAULT_SORT
        const verticalTimeAxis = true

        const fieldNames = [
          {internalName: 'time', displayName: 'Time', visible: true},
          {internalName: 'f1', displayName: '', visible: false},
          {internalName: 'f2', displayName: 'F2', visible: true},
        ]

        const actual = processTableData(
          data,
          sortFieldName,
          direction,
          verticalTimeAxis,
          fieldNames
        )
        const expected = [
          ['time', 'f2'],
          [1000, 2000],
          [2000, 3000],
          [3000, 1000],
        ]

        expect(actual.processedData).toEqual(expected)
      })

      it('filters out invisible columns after sorting', () => {
        const {data} = setup()
        const sortFieldName = 'f1'
        const direction = DEFAULT_SORT
        const verticalTimeAxis = true

        const fieldNames = [
          {internalName: 'time', displayName: 'Time', visible: true},
          {internalName: 'f1', displayName: '', visible: false},
          {internalName: 'f2', displayName: 'F2', visible: true},
        ]

        const actual = processTableData(
          data,
          sortFieldName,
          direction,
          verticalTimeAxis,
          fieldNames
        )
        const expected = [
          ['time', 'f2'],
          [2000, 3000],
          [3000, 1000],
          [1000, 2000],
        ]

        expect(actual.processedData).toEqual(expected)
      })

      describe('if verticalTimeAxis is false', () => {
        it('transforms data', () => {
          const {data} = setup()
          const sortFieldName = 'time'
          const direction = DEFAULT_SORT
          const verticalTimeAxis = false

          const fieldNames = [
            {internalName: 'time', displayName: 'Time', visible: true},
            {internalName: 'f1', displayName: '', visible: true},
            {internalName: 'f2', displayName: 'F2', visible: true},
          ]

          const actual = processTableData(
            data,
            sortFieldName,
            direction,
            verticalTimeAxis,
            fieldNames
          )
          const expected = [
            ['time', 1000, 2000, 3000],
            ['f1', 3000, 1000, 2000],
            ['f2', 2000, 3000, 1000],
          ]

          expect(actual.processedData).toEqual(expected)
        })

        it('transforms data after filtering out invisible columns', () => {
          const {data} = setup()
          const sortFieldName = 'f1'
          const direction = DEFAULT_SORT
          const verticalTimeAxis = false

          const fieldNames = [
            {internalName: 'time', displayName: 'Time', visible: true},
            {internalName: 'f1', displayName: '', visible: false},
            {internalName: 'f2', displayName: 'F2', visible: true},
          ]

          const actual = processTableData(
            data,
            sortFieldName,
            direction,
            verticalTimeAxis,
            fieldNames
          )
          const expected = [
            ['time', 2000, 3000, 1000],
            ['f2', 3000, 1000, 2000],
          ]

          expect(actual.processedData).toEqual(expected)
        })
      })
    })
  })
})
