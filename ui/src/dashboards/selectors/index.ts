import {get} from 'lodash'

import {AppState, View, Check, ViewType, TimeRange} from 'src/types'

import {
  getValuesForVariable,
  getTypeForVariable,
  getArgumentValuesForVariable,
  getSelectedVariableText,
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
  selectedValue: string
}

export const getVariableValuesForDropdown = (
  state: AppState,
  variableID: string,
  contextID: string
): DropdownValues => {
  const {values} = getValuesForVariable(state, variableID, contextID)

  if (!values) {
    return {list: null, selectedValue: null}
  }

  const type = getTypeForVariable(state, variableID)
  const selectedText = getSelectedVariableText(state, variableID, contextID)

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
        selectedValue: selectedText,
        list,
      }
    }
    default:
      const valueCopy = values as string[]
      const list = valueCopy.map(v => ({name: v, value: v}))

      return {selectedValue: selectedText, list}
  }
}
