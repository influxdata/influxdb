import * as CellEditorOverlayActions from 'src/types/actions/cellEditorOverlay'
import * as ColorsModels from 'src/types/colors'
import * as DashboardsModels from 'src/types/dashboards'

export const showCellEditorOverlay: CellEditorOverlayActions.ShowCellEditorOverlayActionCreator = (
  cell: DashboardsModels.Cell
): CellEditorOverlayActions.ShowCellEditorOverlayAction => ({
  type: 'SHOW_CELL_EDITOR_OVERLAY',
  payload: {
    cell,
  },
})

export const hideCellEditorOverlay = (): CellEditorOverlayActions.HideCellEditorOverlayAction => ({
  type: 'HIDE_CELL_EDITOR_OVERLAY',
})

export const changeCellType = (
  cellType: DashboardsModels.CellType
): CellEditorOverlayActions.ChangeCellTypeAction => ({
  type: 'CHANGE_CELL_TYPE',
  payload: {
    cellType,
  },
})

export const renameCell = (
  cellName: string
): CellEditorOverlayActions.RenameCellAction => ({
  type: 'RENAME_CELL',
  payload: {
    cellName,
  },
})

export const updateThresholdsListColors = (
  thresholdsListColors: ColorsModels.ColorNumber[]
): CellEditorOverlayActions.UpdateThresholdsListColorsAction => ({
  type: 'UPDATE_THRESHOLDS_LIST_COLORS',
  payload: {
    thresholdsListColors,
  },
})

export const updateThresholdsListType = (
  thresholdsListType: DashboardsModels.ThresholdType
): CellEditorOverlayActions.UpdateThresholdsListTypeAction => ({
  type: 'UPDATE_THRESHOLDS_LIST_TYPE',
  payload: {
    thresholdsListType,
  },
})

export const updateGaugeColors = (
  gaugeColors: ColorsModels.ColorNumber[]
): CellEditorOverlayActions.UpdateGaugeColorsAction => ({
  type: 'UPDATE_GAUGE_COLORS',
  payload: {
    gaugeColors,
  },
})

export const updateAxes = (
  axes: DashboardsModels.Axes
): CellEditorOverlayActions.UpdateAxesAction => ({
  type: 'UPDATE_AXES',
  payload: {
    axes,
  },
})

export const updateTableOptions = (
  tableOptions: DashboardsModels.TableOptions
): CellEditorOverlayActions.UpdateTableOptionsAction => ({
  type: 'UPDATE_TABLE_OPTIONS',
  payload: {
    tableOptions,
  },
})

export const updateLineColors = (
  lineColors: ColorsModels.ColorString[]
): CellEditorOverlayActions.UpdateLineColorsAction => ({
  type: 'UPDATE_LINE_COLORS',
  payload: {
    lineColors,
  },
})

export const changeTimeFormat = (
  timeFormat: string
): CellEditorOverlayActions.ChangeTimeFormatAction => ({
  type: 'CHANGE_TIME_FORMAT',
  payload: {
    timeFormat,
  },
})

export const changeDecimalPlaces = (
  decimalPlaces: DashboardsModels.DecimalPlaces
): CellEditorOverlayActions.ChangeDecimalPlacesAction => ({
  type: 'CHANGE_DECIMAL_PLACES',
  payload: {
    decimalPlaces,
  },
})

export const updateFieldOptions = (
  fieldOptions: DashboardsModels.FieldOption[]
): CellEditorOverlayActions.UpdateFieldOptionsAction => ({
  type: 'UPDATE_FIELD_OPTIONS',
  payload: {
    fieldOptions,
  },
})
