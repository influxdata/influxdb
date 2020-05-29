// Libraries
import React from 'react'
import {render, fireEvent} from 'react-testing-library'

// Components
import {ThresholdCondition} from 'src/alerting/components/builder/ThresholdCondition'

// Types
import {Table} from '@influxdata/giraffe'
import {CheckStatusLevel} from 'src/types'

describe('ThresholdCondition Builder', () => {
  describe('empty state', () => {
    it('should default to the middle of the graph', () => {
      const onUpdateThreshold = jest.fn()
      const onRemoveThreshold = jest.fn()
      const table = ({
        getColumn() {
          return [0, 0, 1000000]
        },
        getColumnName: jest.fn(),
        getColumnType: jest.fn(),
        addColumn: jest.fn(),
        columnKeys: [],
        length: 3,
      } as unknown) as Table
      const props = {
        onUpdateThreshold,
        onRemoveThreshold,
        table,
        level: 'CRIT' as CheckStatusLevel,
        threshold: null,
      }

      const wrapper = render(<ThresholdCondition {...props} />)
      const {getByTestId} = wrapper

      const button = getByTestId(`add-threshold-condition-${props.level}`)

      fireEvent.click(button)

      expect(onUpdateThreshold.mock.calls.length).toEqual(1)
      expect(onUpdateThreshold.mock.calls[0][0].value).toEqual(500000)
    })
  })
})
