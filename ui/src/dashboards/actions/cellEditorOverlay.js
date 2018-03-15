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

export const updateSingleStatColors = singleStatColors => ({
  type: 'UPDATE_SINGLE_STAT_COLORS',
  payload: {
    singleStatColors,
  },
})

export const updateThresholdsListType = thresholdsListType => ({
  type: 'UPDATE_SINGLE_STAT_TYPE',
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
