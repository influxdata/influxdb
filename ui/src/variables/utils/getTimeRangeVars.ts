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

  if (isDateParseable(timeRange.lower)) {
    startValue = generateDateTimeLiteral(TIME_RANGE_START, timeRange.lower)
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

  if (timeRange.upper && isDateParseable(timeRange.upper)) {
    stopValue = generateDateTimeLiteral(TIME_RANGE_STOP, timeRange.upper)
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

const generateDateTimeLiteral = (
  name: string,
  value: string
): VariableAssignment => {
  return {
    type: 'VariableAssignment',
    id: {
      type: 'Identifier',
      name,
    },
    init: {
      type: 'DateTimeLiteral',
      value: new Date(value).toISOString(),
    },
  }
}

export const isDateParseable = (ambiguousString: string): boolean =>
  !isNaN(Date.parse(ambiguousString))
