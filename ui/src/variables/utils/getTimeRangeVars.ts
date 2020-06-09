// Utils
import {parseDuration, timeRangeToDuration} from 'src/shared/utils/duration'
import {asAssignment} from 'src/variables/selectors'

// Constants
import {TIME_RANGE_START, TIME_RANGE_STOP} from 'src/variables/constants'

// Types
import {RemoteDataState, TimeRange, Variable} from 'src/types'
import {VariableAssignment} from 'src/types/ast'
import {formatTimeRange} from 'src/shared/utils/time'

// TODO kill this function
export const getTimeRangeVars = (
  timeRange: TimeRange
): VariableAssignment[] => {
  return [
    getRangeVariable(TIME_RANGE_START, timeRange),
    getRangeVariable(TIME_RANGE_STOP, timeRange),
  ].map(v => asAssignment(v))
}

export const getRangeVariable = (
  which: string,
  timeRange: TimeRange
): Variable => {
  const range = which === TIME_RANGE_START ? timeRange.lower : timeRange.upper

  if (which === TIME_RANGE_STOP && !timeRange.upper) {
    return {
      orgID: '',
      id: TIME_RANGE_STOP,
      name: TIME_RANGE_STOP,
      arguments: {
        type: 'system',
        values: [formatTimeRange('now()')],
      },
      status: RemoteDataState.Done,
      labels: [],
    }
  }

  if (which === TIME_RANGE_START && timeRange.type !== 'custom') {
    return {
      orgID: '',
      id: which,
      name: which,
      arguments: {
        type: 'system',
        values: [formatTimeRange(timeRange.lower)],
      },
      status: RemoteDataState.Done,
      labels: [],
    }
  }

  if (isNaN(Date.parse(range))) {
    return {
      orgID: '',
      id: which,
      name: which,
      arguments: {
        type: 'system',
        values: [null],
      },
      status: RemoteDataState.Done,
      labels: [],
    }
  }

  return {
    orgID: '',
    id: which,
    name: which,
    arguments: {
      type: 'system',
      values: [range],
    },
    status: RemoteDataState.Done,
    labels: [],
  }
}
