// Utils
import {parseDuration, timeRangeToDuration} from 'src/shared/utils/duration'

// Constants
import {TIME_RANGE_START, TIME_RANGE_STOP} from 'src/variables/constants'

// Types
import {RemoteDataState, TimeRange, Variable} from 'src/types'
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

export const getTimeRangeAsVariable = (timeRange: TimeRange): Variable[] => {
  let startValue: Variable

  if (!isDateParseable(timeRange.lower)) {
    const [{magnitude, unit}] = parseDuration(timeRangeToDuration(timeRange))
    const start = `${magnitude}${unit}`
    startValue = {
      orgID: '',
      id: TIME_RANGE_START,
      name: TIME_RANGE_START,
      arguments: {
        type: 'map',
        values: {
          [start]: start,
        },
      },
      status: RemoteDataState.Done,
      labels: [],
    }
  }

  let stopValue: Variable

  if (!timeRange.upper) {
    const now = 'now()'
    stopValue = {
      orgID: '',
      id: TIME_RANGE_STOP,
      name: TIME_RANGE_STOP,
      arguments: {
        type: 'map',
        values: {
          [now]: now,
        },
      },
      status: RemoteDataState.Done,
      labels: [],
    }
  }
  const range = []
  if (startValue) {
    range[0] = startValue
  }
  if (stopValue) {
    range[1] = stopValue
  }
  return range
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
