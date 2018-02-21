import {
  SINGLE_STAT_TEXT,
  DEFAULT_SINGLESTAT_COLORS,
  DEFAULT_GAUGE_COLORS,
  validateGaugeColors,
  validateSingleStatColors,
  getSingleStatType,
} from 'src/dashboards/constants/gaugeColors'

const initialState = {
  cell: null,
  singleStatType: SINGLE_STAT_TEXT,
  singleStatColors: DEFAULT_SINGLESTAT_COLORS,
  gaugeColors: DEFAULT_GAUGE_COLORS,
}

export default function cellEditorOverlay(state = initialState, action) {
  switch (action.type) {
    case 'SHOW_CELL_EDITOR_OVERLAY': {
      const {cell, cell: {colors}} = action.payload

      const singleStatType = getSingleStatType(colors)
      const singleStatColors = validateSingleStatColors(colors, singleStatType)
      const gaugeColors = validateGaugeColors(colors)

      return {...state, cell, singleStatType, singleStatColors, gaugeColors}
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
      const {singleStatType} = action.payload

      const singleStatColors = state.singleStatColors.map(color => ({
        ...color,
        type: singleStatType,
      }))

      return {...state, singleStatType, singleStatColors}
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
  }

  return state
}
