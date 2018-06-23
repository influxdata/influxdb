import {getDeep} from 'src/utils/wrappers'

import {
  DEFAULT_THRESHOLDS_LIST_COLORS,
  DEFAULT_GAUGE_COLORS,
  validateGaugeColors,
  validateThresholdsListColors,
  getThresholdsListType,
} from 'src/shared/constants/thresholds'

import {
  DEFAULT_LINE_COLORS,
  validateLineColors,
} from 'src/shared/constants/graphColorPalettes'

import {initializeOptions} from 'src/dashboards/constants/cellEditor'
import {Action} from 'src/types/actions/cellEditorOverlay'
import {CellType, Cell} from 'src/types'
import {ThresholdType, TableOptions} from 'src/types/dashboards'
import {ThresholdColor, GaugeColor, LineColor} from 'src/types/colors'

interface CEOInitialState {
  cell: Cell | null
  thresholdsListType: ThresholdType
  thresholdsListColors: ThresholdColor[]
  gaugeColors: GaugeColor[]
  lineColors: LineColor[]
}

export const initialState = {
  cell: null,
  thresholdsListType: ThresholdType.Text,
  thresholdsListColors: DEFAULT_THRESHOLDS_LIST_COLORS,
  gaugeColors: DEFAULT_GAUGE_COLORS,
  lineColors: DEFAULT_LINE_COLORS,
}

const cellEditorOverlay = (
  state = initialState,
  action: Action
): CEOInitialState => {
  switch (action.type) {
    case 'SHOW_CELL_EDITOR_OVERLAY': {
      const {
        cell,
        cell: {colors},
      } = action.payload

      const thresholdsListType = getThresholdsListType(colors)
      const thresholdsListColors = validateThresholdsListColors(
        colors,
        thresholdsListType
      )
      const gaugeColors = validateGaugeColors(colors)

      const tableOptions = getDeep<TableOptions>(
        cell,
        'tableOptions',
        initializeOptions(CellType.Table)
      )

      const lineColors = validateLineColors(colors)

      return {
        ...state,
        cell: {...cell, tableOptions},
        thresholdsListType,
        thresholdsListColors,
        gaugeColors,
        lineColors,
      }
    }

    case 'HIDE_CELL_EDITOR_OVERLAY': {
      const cell = null

      return {...state, cell}
    }

    case 'CHANGE_CELL_TYPE': {
      const {cellType} = action.payload
      const cell = {...state.cell, type: cellType}

      return {...state, cell}
    }

    case 'RENAME_CELL': {
      const {cellName} = action.payload
      const cell = {...state.cell, name: cellName}

      return {...state, cell}
    }

    case 'UPDATE_THRESHOLDS_LIST_COLORS': {
      const {thresholdsListColors} = action.payload

      return {...state, thresholdsListColors}
    }

    case 'UPDATE_THRESHOLDS_LIST_TYPE': {
      const {thresholdsListType} = action.payload

      const thresholdsListColors = state.thresholdsListColors.map(color => ({
        ...color,
        type: thresholdsListType,
      }))

      return {...state, thresholdsListType, thresholdsListColors}
    }

    case 'UPDATE_GAUGE_COLORS': {
      const {gaugeColors} = action.payload

      return {...state, gaugeColors}
    }

    case 'UPDATE_AXES': {
      const {axes} = action.payload
      const cell = {...state.cell, axes}

      return {...state, cell}
    }

    case 'UPDATE_TABLE_OPTIONS': {
      const {tableOptions} = action.payload
      const cell = {...state.cell, tableOptions}

      return {...state, cell}
    }

    case 'CHANGE_TIME_FORMAT': {
      const {timeFormat} = action.payload
      const cell = {...state.cell, timeFormat}

      return {...state, cell}
    }
    case 'CHANGE_DECIMAL_PLACES': {
      const {decimalPlaces} = action.payload
      const cell = {...state.cell, decimalPlaces}

      return {...state, cell}
    }
    case 'UPDATE_FIELD_OPTIONS': {
      const {fieldOptions} = action.payload
      const cell = {...state.cell, fieldOptions}

      return {...state, cell}
    }

    case 'UPDATE_LINE_COLORS': {
      const {lineColors} = action.payload

      return {...state, lineColors}
    }
  }

  return state
}

export default cellEditorOverlay
