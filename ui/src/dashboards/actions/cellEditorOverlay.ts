import {ColorNumber, ColorString} from 'src/types/colors'
import {
  DecimalPlaces,
  FieldOption,
  Axes,
  Cell,
  CellType,
  ThresholdType,
  TableOptions,
} from 'src/types/dashboards'

export enum ActionType {
  ShowCellEditorOverlay = 'SHOW_CELL_EDITOR_OVERLAY',
  HideCellEditorOverlay = 'HIDE_CELL_EDITOR_OVERLAY',
  ChangeCellType = 'CHANGE_CELL_TYPE',
  RenameCell = 'RENAME_CELL',
  UpdateThresholdsListColors = 'UPDATE_THRESHOLDS_LIST_COLORS',
  UpdateThresholdsListType = 'UPDATE_THRESHOLDS_LIST_TYPE',
  UpdateGaugeColors = 'UPDATE_GAUGE_COLORS',
  UpdateAxes = 'UPDATE_AXES',
  UpdateTableOptions = 'UPDATE_TABLE_OPTIONS',
  UpdateLineColors = 'UPDATE_LINE_COLORS',
  ChangeTimeFormat = 'CHANGE_TIME_FORMAT',
  ChangeDecimalPlaces = 'CHANGE_DECIMAL_PLACES',
  UpdateFieldOptions = 'UPDATE_FIELD_OPTIONS',
}

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

export interface ShowCellEditorOverlayAction {
  type: ActionType.ShowCellEditorOverlay
  payload: {
    cell: Cell
  }
}

export interface HideCellEditorOverlayAction {
  type: ActionType.HideCellEditorOverlay
}

export interface ChangeCellTypeAction {
  type: ActionType.ChangeCellType
  payload: {
    cellType: CellType
  }
}

export interface RenameCellAction {
  type: ActionType.RenameCell
  payload: {
    cellName: string
  }
}

export interface UpdateThresholdsListColorsAction {
  type: ActionType.UpdateThresholdsListColors
  payload: {
    thresholdsListColors: ColorNumber[]
  }
}

export interface UpdateThresholdsListTypeAction {
  type: ActionType.UpdateThresholdsListType
  payload: {
    thresholdsListType: ThresholdType
  }
}

export interface UpdateGaugeColorsAction {
  type: ActionType.UpdateGaugeColors
  payload: {
    gaugeColors: ColorNumber[]
  }
}

export interface UpdateAxesAction {
  type: ActionType.UpdateAxes
  payload: {
    axes: Axes
  }
}

export interface UpdateTableOptionsAction {
  type: ActionType.UpdateTableOptions
  payload: {
    tableOptions: TableOptions
  }
}

export interface UpdateLineColorsAction {
  type: ActionType.UpdateLineColors
  payload: {
    lineColors: ColorString[]
  }
}

export interface ChangeTimeFormatAction {
  type: ActionType.ChangeTimeFormat
  payload: {
    timeFormat: string
  }
}

export interface ChangeDecimalPlacesAction {
  type: ActionType.ChangeDecimalPlaces
  payload: {
    decimalPlaces: DecimalPlaces
  }
}

export interface UpdateFieldOptionsAction {
  type: ActionType.UpdateFieldOptions
  payload: {
    fieldOptions: FieldOption[]
  }
}

export const showCellEditorOverlay = (
  cell: Cell
): ShowCellEditorOverlayAction => ({
  type: ActionType.ShowCellEditorOverlay,
  payload: {
    cell,
  },
})

export const hideCellEditorOverlay = (): HideCellEditorOverlayAction => ({
  type: ActionType.HideCellEditorOverlay,
})

export const changeCellType = (cellType: CellType): ChangeCellTypeAction => ({
  type: ActionType.ChangeCellType,
  payload: {
    cellType,
  },
})

export const renameCell = (cellName: string): RenameCellAction => ({
  type: ActionType.RenameCell,
  payload: {
    cellName,
  },
})

export const updateThresholdsListColors = (
  thresholdsListColors: ColorNumber[]
): UpdateThresholdsListColorsAction => ({
  type: ActionType.UpdateThresholdsListColors,
  payload: {
    thresholdsListColors,
  },
})

export const updateThresholdsListType = (
  thresholdsListType: ThresholdType
): UpdateThresholdsListTypeAction => ({
  type: ActionType.UpdateThresholdsListType,
  payload: {
    thresholdsListType,
  },
})

export const updateGaugeColors = (
  gaugeColors: ColorNumber[]
): UpdateGaugeColorsAction => ({
  type: ActionType.UpdateGaugeColors,
  payload: {
    gaugeColors,
  },
})

export const updateAxes = (axes: Axes): UpdateAxesAction => ({
  type: ActionType.UpdateAxes,
  payload: {
    axes,
  },
})

export const updateTableOptions = (
  tableOptions: TableOptions
): UpdateTableOptionsAction => ({
  type: ActionType.UpdateTableOptions,
  payload: {
    tableOptions,
  },
})

export const updateLineColors = (
  lineColors: ColorString[]
): UpdateLineColorsAction => ({
  type: ActionType.UpdateLineColors,
  payload: {
    lineColors,
  },
})

export const changeTimeFormat = (
  timeFormat: string
): ChangeTimeFormatAction => ({
  type: ActionType.ChangeTimeFormat,
  payload: {
    timeFormat,
  },
})

export const changeDecimalPlaces = (
  decimalPlaces: DecimalPlaces
): ChangeDecimalPlacesAction => ({
  type: ActionType.ChangeDecimalPlaces,
  payload: {
    decimalPlaces,
  },
})

export const updateFieldOptions = (
  fieldOptions: FieldOption[]
): UpdateFieldOptionsAction => ({
  type: ActionType.UpdateFieldOptions,
  payload: {
    fieldOptions,
  },
})
