// Utils
import {parseDuration, timeRangeToDuration} from 'src/shared/utils/duration'

// Constants
import {TIME_RANGE_START, TIME_RANGE_STOP} from 'src/variables/constants'

// Types
import {TimeRange} from 'src/types'
import {VariableAssignment} from 'src/types/ast'

export const getTimeRangeVars = (
  timeRange: TimeRange
): VariableAssignment[] => {
  let startValue: VariableAssignment

  if (isDate(timeRange.lower)) {
    startValue = {
      type: 'VariableAssignment',
      id: {
        type: 'Identifier',
        name: TIME_RANGE_START,
      },
      init: {
        type: 'DateTimeLiteral',
        value: new Date(timeRange.lower).toISOString(),
      },
    }
  } else {
    startValue = {
      type: 'VariableAssignment',
      id: {
        type: 'Identifier',
        name: TIME_RANGE_START,
      },
      init: {
        type: 'UnaryExpression',
        operator: '-',
        argument: {
          type: 'DurationLiteral',
          values: parseDuration(timeRangeToDuration(timeRange)),
        },
      },
    }
  }

  let stopValue: VariableAssignment

  if (timeRange.upper && isDate(timeRange.upper)) {
    stopValue = {
      type: 'VariableAssignment',
      id: {
        type: 'Identifier',
        name: TIME_RANGE_STOP,
      },
      init: {
        type: 'DateTimeLiteral',
        value: new Date(timeRange.upper).toISOString(),
      },
    }
  } else {
    stopValue = {
      type: 'VariableAssignment',
      id: {
        type: 'Identifier',
        name: TIME_RANGE_STOP,
      },
      init: {
        type: 'CallExpression',
        callee: {
          type: 'Identifier',
          name: 'now',
        },
      },
    }
  }

  return [startValue, stopValue]
}

const isDate = (ambiguousString: string): boolean =>
  !isNaN(Date.parse(ambiguousString))
