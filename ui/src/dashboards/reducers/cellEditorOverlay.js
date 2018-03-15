import {
  THRESHOLD_TYPE_TEXT,
  DEFAULT_SINGLESTAT_COLORS,
  DEFAULT_GAUGE_COLORS,
  validateGaugeColors,
  validateSingleStatColors,
  getThresholdsListType,
} from 'shared/constants/thresholds'

import {initializeOptions} from 'src/dashboards/constants/cellEditor'

export const initialState = {
  cell: null,
  thresholdsListType: THRESHOLD_TYPE_TEXT,
  singleStatColors: DEFAULT_SINGLESTAT_COLORS,
  gaugeColors: DEFAULT_GAUGE_COLORS,
}

export default function cellEditorOverlay(state = initialState, action) {
  switch (action.type) {
    case 'SHOW_CELL_EDITOR_OVERLAY': {
      const {cell, cell: {colors}} = action.payload

      const thresholdsListType = getThresholdsListType(colors)
      const singleStatColors = validateSingleStatColors(
        colors,
        thresholdsListType
      )
      const gaugeColors = validateGaugeColors(colors)

      const tableOptions = cell.tableOptions || initializeOptions('table')

      return {
        ...state,
        cell: {...cell, tableOptions},
        thresholdsListType,
        singleStatColors,
        gaugeColors,
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

    case 'UPDATE_SINGLE_STAT_COLORS': {
      const {singleStatColors} = action.payload

      return {...state, singleStatColors}
    }

    case 'UPDATE_SINGLE_STAT_TYPE': {
      const {thresholdsListType} = action.payload

      const singleStatColors = state.singleStatColors.map(color => ({
        ...color,
        type: thresholdsListType,
      }))

      return {...state, thresholdsListType, singleStatColors}
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
  }

  return state
}
