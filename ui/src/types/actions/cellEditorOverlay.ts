import * as Types from 'src/types/modules'

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
  cell: Types.Dashboards.Data.Cell
) => ShowCellEditorOverlayAction

export interface ShowCellEditorOverlayAction {
  type: 'SHOW_CELL_EDITOR_OVERLAY'
  payload: {
    cell: Types.Dashboards.Data.Cell
  }
}

export type HideCellEditorOverlayActionCreator = () => HideCellEditorOverlayAction

export interface HideCellEditorOverlayAction {
  type: 'HIDE_CELL_EDITOR_OVERLAY'
}

export interface ChangeCellTypeAction {
  type: 'CHANGE_CELL_TYPE'
  payload: {
    cellType: Types.Dashboards.Data.CellType
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
    thresholdsListColors: Types.Colors.Data.ColorNumber[]
  }
}

export interface UpdateThresholdsListTypeAction {
  type: 'UPDATE_THRESHOLDS_LIST_TYPE'
  payload: {
    thresholdsListType: Types.Dashboards.Data.ThresholdType
  }
}

export interface UpdateGaugeColorsAction {
  type: 'UPDATE_GAUGE_COLORS'
  payload: {
    gaugeColors: Types.Colors.Data.ColorNumber[]
  }
}

export interface UpdateAxesAction {
  type: 'UPDATE_AXES'
  payload: {
    axes: Types.Dashboards.Data.Axes
  }
}

export interface UpdateTableOptionsAction {
  type: 'UPDATE_TABLE_OPTIONS'
  payload: {
    tableOptions: Types.Dashboards.Data.TableOptions
  }
}

export interface UpdateLineColorsAction {
  type: 'UPDATE_LINE_COLORS'
  payload: {
    lineColors: Types.Colors.Data.ColorString[]
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
    decimalPlaces: Types.Dashboards.Data.DecimalPlaces
  }
}

export interface UpdateFieldOptionsAction {
  type: 'UPDATE_FIELD_OPTIONS'
  payload: {
    fieldOptions: Types.Dashboards.Data.FieldOption[]
  }
}
