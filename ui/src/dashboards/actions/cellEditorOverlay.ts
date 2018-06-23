import {Cell} from 'src/types'
import {CellType, ThresholdType} from 'src/types/dashboards'
import {ColorNumber, ColorString} from 'src/types/colors'
import {
  Axes,
  DecimalPlaces,
  FieldOption,
  TableOptions,
} from 'src/types/dashboards'

export type Action =
  | ShowCellEditorOverlayAction
  | HideCellEditorOverlayAction
  | ChangeCellTypeAction
  | RenameCellAction
  | UpdateThresholdsListColorsAction
  | UpdateThresholdsListTypeAction
  | UpdateGaugeColorsAction
  | UpdateAxesAction
  | UpdateTableOptionsAction
  | UpdateLineColorsAction
  | ChangeTimeFormatAction
  | ChangeDecimalPlacesAction
  | UpdateFieldOptionsAction

export type ShowCellEditorOverlayActionCreator = (
  cell: Cell
) => ShowCellEditorOverlayAction

interface ShowCellEditorOverlayAction {
  type: 'SHOW_CELL_EDITOR_OVERLAY'
  payload: {
    cell: Cell
  }
}
export const showCellEditorOverlay: ShowCellEditorOverlayActionCreator = (
  cell: Cell
): ShowCellEditorOverlayAction => ({
  type: 'SHOW_CELL_EDITOR_OVERLAY',
  payload: {
    cell,
  },
})

export type HideCellEditorOverlayActionCreator = () => HideCellEditorOverlayAction

interface HideCellEditorOverlayAction {
  type: 'HIDE_CELL_EDITOR_OVERLAY'
}
export const hideCellEditorOverlay = (): HideCellEditorOverlayAction => ({
  type: 'HIDE_CELL_EDITOR_OVERLAY',
})

interface ChangeCellTypeAction {
  type: 'CHANGE_CELL_TYPE'
  payload: {
    cellType: CellType
  }
}

export const changeCellType = (cellType: CellType): ChangeCellTypeAction => ({
  type: 'CHANGE_CELL_TYPE',
  payload: {
    cellType,
  },
})

interface RenameCellAction {
  type: 'RENAME_CELL'
  payload: {
    cellName: string
  }
}
export const renameCell = (cellName: string): RenameCellAction => ({
  type: 'RENAME_CELL',
  payload: {
    cellName,
  },
})

interface UpdateThresholdsListColorsAction {
  type: 'UPDATE_THRESHOLDS_LIST_COLORS'
  payload: {
    thresholdsListColors: ColorNumber[]
  }
}
export const updateThresholdsListColors = (
  thresholdsListColors: ColorNumber[]
): UpdateThresholdsListColorsAction => ({
  type: 'UPDATE_THRESHOLDS_LIST_COLORS',
  payload: {
    thresholdsListColors,
  },
})

interface UpdateThresholdsListTypeAction {
  type: 'UPDATE_THRESHOLDS_LIST_TYPE'
  payload: {
    thresholdsListType: ThresholdType
  }
}

export const updateThresholdsListType = (
  thresholdsListType: ThresholdType
): UpdateThresholdsListTypeAction => ({
  type: 'UPDATE_THRESHOLDS_LIST_TYPE',
  payload: {
    thresholdsListType,
  },
})

interface UpdateGaugeColorsAction {
  type: 'UPDATE_GAUGE_COLORS'
  payload: {
    gaugeColors: ColorNumber[]
  }
}
export const updateGaugeColors = (
  gaugeColors: ColorNumber[]
): UpdateGaugeColorsAction => ({
  type: 'UPDATE_GAUGE_COLORS',
  payload: {
    gaugeColors,
  },
})

interface UpdateAxesAction {
  type: 'UPDATE_AXES'
  payload: {
    axes: Axes
  }
}
export const updateAxes = (axes: Axes): UpdateAxesAction => ({
  type: 'UPDATE_AXES',
  payload: {
    axes,
  },
})

interface UpdateTableOptionsAction {
  type: 'UPDATE_TABLE_OPTIONS'
  payload: {
    tableOptions: TableOptions
  }
}
export const updateTableOptions = (
  tableOptions: TableOptions
): UpdateTableOptionsAction => ({
  type: 'UPDATE_TABLE_OPTIONS',
  payload: {
    tableOptions,
  },
})

interface UpdateLineColorsAction {
  type: 'UPDATE_LINE_COLORS'
  payload: {
    lineColors: ColorString[]
  }
}
export const updateLineColors = (
  lineColors: ColorString[]
): UpdateLineColorsAction => ({
  type: 'UPDATE_LINE_COLORS',
  payload: {
    lineColors,
  },
})

interface ChangeTimeFormatAction {
  type: 'CHANGE_TIME_FORMAT'
  payload: {
    timeFormat: string
  }
}
export const changeTimeFormat = (
  timeFormat: string
): ChangeTimeFormatAction => ({
  type: 'CHANGE_TIME_FORMAT',
  payload: {
    timeFormat,
  },
})

interface ChangeDecimalPlacesAction {
  type: 'CHANGE_DECIMAL_PLACES'
  payload: {
    decimalPlaces: DecimalPlaces
  }
}
export const changeDecimalPlaces = (
  decimalPlaces: DecimalPlaces
): ChangeDecimalPlacesAction => ({
  type: 'CHANGE_DECIMAL_PLACES',
  payload: {
    decimalPlaces,
  },
})

interface UpdateFieldOptionsAction {
  type: 'UPDATE_FIELD_OPTIONS'
  payload: {
    fieldOptions: FieldOption[]
  }
}
export const updateFieldOptions = (
  fieldOptions: FieldOption[]
): UpdateFieldOptionsAction => ({
  type: 'UPDATE_FIELD_OPTIONS',
  payload: {
    fieldOptions,
  },
})
