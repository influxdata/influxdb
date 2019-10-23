// Libraries
import React from 'react'
import {render, fireEvent} from 'react-testing-library'

// Components
import {ThresholdCondition} from 'src/alerting/components/builder/ThresholdCondition'

describe('ThresholdCondition', () => {
  describe('empty state', () => {
    it('should default to the middle of the graph', () => {
      const onUpdateCheckThreshold = jest.fn()
      const onRemoveCheckThreshold = jest.fn()
      const table = {
        getColumn() {
          return [0, 0, 1000000]
        },
      }
      const props = {
        onUpdateCheckThreshold,
        onRemoveCheckThreshold,
        table,
        level: 'critical',
      }

      const wrapper = render(<ThresholdCondition {...props} />)
      const {getByTestId} = wrapper

      const button = getByTestId(`add-threshold-condition-${props.level}`)

      fireEvent.click(button)

      expect(onUpdateCheckThreshold.mock.calls.length).toEqual(1)
      expect(onUpdateCheckThreshold.mock.calls[0][0].value).toEqual(200000)
    })
  })
})
