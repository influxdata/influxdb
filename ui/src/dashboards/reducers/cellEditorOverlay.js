const initialState = {
  cell: null,
}

export default function cellEditorOverlay(state = initialState, action) {
  switch (action.type) {
    case 'SHOW_CELL_EDITOR_OVERLAY': {
      const {cell} = action.payload

      return {...state, cell}
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
  }

  return state
}
