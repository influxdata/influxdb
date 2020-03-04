import {get} from 'lodash'

import {AppState, View, Check, ViewType, TimeRange} from 'src/types'

import {
  getValuesForVariable,
  getTypeForVariable,
  getArgumentValuesForVariable,
} from 'src/variables/selectors'

// Constants
import {DEFAULT_TIME_RANGE} from 'src/shared/constants/timeRanges'

export const getTimeRangeByDashboardID = (
  state: AppState,
  dashboardID: string
): TimeRange => state.ranges[dashboardID] || DEFAULT_TIME_RANGE

export const getCheckForView = (
  state: AppState,
  view: View
): Partial<Check> => {
  const viewType: ViewType = get(view, 'properties.type')
  const checkID = get(view, 'properties.checkID')

  return viewType === 'check' ? state.resources.checks.byID[checkID] : null
}

interface DropdownValues {
  list: {name: string; value: string}[]
  selectedKey: string
}

export const getVariableValuesForDropdown = (
  state: AppState,
  variableID: string,
  contextID: string
): DropdownValues => {
  const {selectedKey, values} = getValuesForVariable(
    state,
    variableID,
    contextID
  )

  if (!values) {
    return {list: null, selectedKey: null}
  }

  const type = getTypeForVariable(state, variableID)

  switch (type) {
    case 'map': {
      const mapValues = getArgumentValuesForVariable(state, variableID) as {
        [key: string]: string
      }

      const list = Object.entries(mapValues).map(([name, value]) => ({
        name,
        value,
      }))

      return {
        selectedKey,
        list,
      }
    }
    default:
      const valueCopy = values as string[]
      const list = valueCopy.map(v => ({name: v, value: v}))

      return {selectedKey, list}
  }
}
