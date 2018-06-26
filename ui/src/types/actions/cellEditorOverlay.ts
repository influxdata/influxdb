import * as ColorsModels from 'src/types/colors'
import * as DashboardsModels from 'src/types/dashboards'

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
  cell: DashboardsModels.Cell
) => ShowCellEditorOverlayAction

export interface ShowCellEditorOverlayAction {
  type: 'SHOW_CELL_EDITOR_OVERLAY'
  payload: {
    cell: DashboardsModels.Cell
  }
}

export type HideCellEditorOverlayActionCreator = () => HideCellEditorOverlayAction

export interface HideCellEditorOverlayAction {
  type: 'HIDE_CELL_EDITOR_OVERLAY'
}

export interface ChangeCellTypeAction {
  type: 'CHANGE_CELL_TYPE'
  payload: {
    cellType: DashboardsModels.CellType
  }
}

export interface RenameCellAction {
  type: 'RENAME_CELL'
  payload: {
    cellName: string
  }
}

export interface UpdateThresholdsListColorsAction {
  type: 'UPDATE_THRESHOLDS_LIST_COLORS'
  payload: {
    thresholdsListColors: ColorsModels.ColorNumber[]
  }
}

export interface UpdateThresholdsListTypeAction {
  type: 'UPDATE_THRESHOLDS_LIST_TYPE'
  payload: {
    thresholdsListType: DashboardsModels.ThresholdType
  }
}

export interface UpdateGaugeColorsAction {
  type: 'UPDATE_GAUGE_COLORS'
  payload: {
    gaugeColors: ColorsModels.ColorNumber[]
  }
}

export interface UpdateAxesAction {
  type: 'UPDATE_AXES'
  payload: {
    axes: DashboardsModels.Axes
  }
}

export interface UpdateTableOptionsAction {
  type: 'UPDATE_TABLE_OPTIONS'
  payload: {
    tableOptions: DashboardsModels.TableOptions
  }
}

export interface UpdateLineColorsAction {
  type: 'UPDATE_LINE_COLORS'
  payload: {
    lineColors: ColorsModels.ColorString[]
  }
}

export interface ChangeTimeFormatAction {
  type: 'CHANGE_TIME_FORMAT'
  payload: {
    timeFormat: string
  }
}

export interface ChangeDecimalPlacesAction {
  type: 'CHANGE_DECIMAL_PLACES'
  payload: {
    decimalPlaces: DashboardsModels.DecimalPlaces
  }
}

export interface UpdateFieldOptionsAction {
  type: 'UPDATE_FIELD_OPTIONS'
  payload: {
    fieldOptions: DashboardsModels.FieldOption[]
  }
}
