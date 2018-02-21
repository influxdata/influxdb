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
