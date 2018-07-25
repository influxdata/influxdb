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
import {Action, ActionType} from 'src/dashboards/actions/cellEditorOverlay'
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

export default (state = initialState, action: Action): CEOInitialState => {
  switch (action.type) {
    case ActionType.ShowCellEditorOverlay: {
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

    case ActionType.HideCellEditorOverlay: {
      const cell = null

      return {...state, cell}
    }

    case ActionType.ChangeCellType: {
      const {cellType} = action.payload
      const cell = {...state.cell, type: cellType}

      return {...state, cell}
    }

    case ActionType.RenameCell: {
      const {cellName} = action.payload
      const cell = {...state.cell, name: cellName}

      return {...state, cell}
    }

    case ActionType.UpdateThresholdsListColors: {
      const {thresholdsListColors} = action.payload

      return {...state, thresholdsListColors}
    }

    case ActionType.UpdateThresholdsListType: {
      const {thresholdsListType} = action.payload

      const thresholdsListColors = state.thresholdsListColors.map(color => ({
        ...color,
        type: thresholdsListType,
      }))

      return {...state, thresholdsListType, thresholdsListColors}
    }

    case ActionType.UpdateGaugeColors: {
      const {gaugeColors} = action.payload

      return {...state, gaugeColors}
    }

    case ActionType.UpdateAxes: {
      const {axes} = action.payload
      const cell = {...state.cell, axes}

      return {...state, cell}
    }

    case ActionType.UpdateTableOptions: {
      const {tableOptions} = action.payload
      const cell = {...state.cell, tableOptions}

      return {...state, cell}
    }

    case ActionType.ChangeTimeFormat: {
      const {timeFormat} = action.payload
      const cell = {...state.cell, timeFormat}

      return {...state, cell}
    }

    case ActionType.ChangeDecimalPlaces: {
      const {decimalPlaces} = action.payload
      const cell = {...state.cell, decimalPlaces}

      return {...state, cell}
    }

    case ActionType.UpdateFieldOptions: {
      const {fieldOptions} = action.payload
      const cell = {...state.cell, fieldOptions}

      return {...state, cell}
    }

    case ActionType.UpdateLineColors: {
      const {lineColors} = action.payload

      return {...state, lineColors}
    }
  }

  return state
}
