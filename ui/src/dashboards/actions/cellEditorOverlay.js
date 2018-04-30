export const showCellEditorOverlay = cell => ({
  type: 'SHOW_CELL_EDITOR_OVERLAY',
  payload: {
    cell,
  },
})

export const hideCellEditorOverlay = () => ({
  type: 'HIDE_CELL_EDITOR_OVERLAY',
})

export const changeCellType = cellType => ({
  type: 'CHANGE_CELL_TYPE',
  payload: {
    cellType,
  },
})

export const renameCell = cellName => ({
  type: 'RENAME_CELL',
  payload: {
    cellName,
  },
})

export const updateThresholdsListColors = thresholdsListColors => ({
  type: 'UPDATE_THRESHOLDS_LIST_COLORS',
  payload: {
    thresholdsListColors,
  },
})

export const updateThresholdsListType = thresholdsListType => ({
  type: 'UPDATE_THRESHOLDS_LIST_TYPE',
  payload: {
    thresholdsListType,
  },
})

export const updateGaugeColors = gaugeColors => ({
  type: 'UPDATE_GAUGE_COLORS',
  payload: {
    gaugeColors,
  },
})

export const updateAxes = axes => ({
  type: 'UPDATE_AXES',
  payload: {
    axes,
  },
})

export const updateTableOptions = tableOptions => ({
  type: 'UPDATE_TABLE_OPTIONS',
  payload: {
    tableOptions,
  },
})

export const updateLineColors = lineColors => ({
  type: 'UPDATE_LINE_COLORS',
  payload: {
    lineColors,
  },
})

export const changeTimeFormat = timeFormat => ({
  type: 'CHANGE_TIME_FORMAT',
  payload: {
    timeFormat,
  },
})

export const changeDecimalPlaces = decimalPlaces => ({
  type: 'CHANGE_DECIMAL_PLACES',
  payload: {
    decimalPlaces,
  },
})

export const updateFieldOptions = fieldOptions => ({
  type: 'UPDATE_FIELD_OPTIONS',
  payload: {
    fieldOptions,
  },
})
