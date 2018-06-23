import * as Types from 'src/types/modules'

export const showCellEditorOverlay: Types.CellEditorOverlay.Actions.ShowCellEditorOverlayActionCreator = (
  cell: Types.Dashboards.Data.Cell
): Types.CellEditorOverlay.Actions.ShowCellEditorOverlayAction => ({
  type: 'SHOW_CELL_EDITOR_OVERLAY',
  payload: {
    cell,
  },
})

export const hideCellEditorOverlay = (): Types.CellEditorOverlay.Actions.HideCellEditorOverlayAction => ({
  type: 'HIDE_CELL_EDITOR_OVERLAY',
})

export const changeCellType = (
  cellType: Types.Dashboards.Data.CellType
): Types.CellEditorOverlay.Actions.ChangeCellTypeAction => ({
  type: 'CHANGE_CELL_TYPE',
  payload: {
    cellType,
  },
})

export const renameCell = (
  cellName: string
): Types.CellEditorOverlay.Actions.RenameCellAction => ({
  type: 'RENAME_CELL',
  payload: {
    cellName,
  },
})

export const updateThresholdsListColors = (
  thresholdsListColors: Types.Colors.Data.ColorNumber[]
): Types.CellEditorOverlay.Actions.UpdateThresholdsListColorsAction => ({
  type: 'UPDATE_THRESHOLDS_LIST_COLORS',
  payload: {
    thresholdsListColors,
  },
})

export const updateThresholdsListType = (
  thresholdsListType: Types.Dashboards.Data.ThresholdType
): Types.CellEditorOverlay.Actions.UpdateThresholdsListTypeAction => ({
  type: 'UPDATE_THRESHOLDS_LIST_TYPE',
  payload: {
    thresholdsListType,
  },
})

export const updateGaugeColors = (
  gaugeColors: Types.Colors.Data.ColorNumber[]
): Types.CellEditorOverlay.Actions.UpdateGaugeColorsAction => ({
  type: 'UPDATE_GAUGE_COLORS',
  payload: {
    gaugeColors,
  },
})

export const updateAxes = (
  axes: Types.Dashboards.Data.Axes
): Types.CellEditorOverlay.Actions.UpdateAxesAction => ({
  type: 'UPDATE_AXES',
  payload: {
    axes,
  },
})

export const updateTableOptions = (
  tableOptions: Types.Dashboards.Data.TableOptions
): Types.CellEditorOverlay.Actions.UpdateTableOptionsAction => ({
  type: 'UPDATE_TABLE_OPTIONS',
  payload: {
    tableOptions,
  },
})

export const updateLineColors = (
  lineColors: Types.Colors.Data.ColorString[]
): Types.CellEditorOverlay.Actions.UpdateLineColorsAction => ({
  type: 'UPDATE_LINE_COLORS',
  payload: {
    lineColors,
  },
})

export const changeTimeFormat = (
  timeFormat: string
): Types.CellEditorOverlay.Actions.ChangeTimeFormatAction => ({
  type: 'CHANGE_TIME_FORMAT',
  payload: {
    timeFormat,
  },
})

export const changeDecimalPlaces = (
  decimalPlaces: Types.Dashboards.Data.DecimalPlaces
): Types.CellEditorOverlay.Actions.ChangeDecimalPlacesAction => ({
  type: 'CHANGE_DECIMAL_PLACES',
  payload: {
    decimalPlaces,
  },
})

export const updateFieldOptions = (
  fieldOptions: Types.Dashboards.Data.FieldOption[]
): Types.CellEditorOverlay.Actions.UpdateFieldOptionsAction => ({
  type: 'UPDATE_FIELD_OPTIONS',
  payload: {
    fieldOptions,
  },
})
