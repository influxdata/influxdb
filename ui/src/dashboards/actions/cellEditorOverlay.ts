import {Cell} from 'src/types'
import {ColorNumber, ColorString} from 'src/types/colors'
import * as DashboardData from 'src/types/dashboards'
import * as CellEditorOverlayActions from 'src/types/actions/cellEditorOverlay'

export const showCellEditorOverlay: CellEditorOverlayActions.ShowCellEditorOverlayActionCreator = (
  cell: Cell
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
  cellType: DashboardData.CellType
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
  thresholdsListColors: ColorNumber[]
): CellEditorOverlayActions.UpdateThresholdsListColorsAction => ({
  type: 'UPDATE_THRESHOLDS_LIST_COLORS',
  payload: {
    thresholdsListColors,
  },
})

export const updateThresholdsListType = (
  thresholdsListType: DashboardData.ThresholdType
): CellEditorOverlayActions.UpdateThresholdsListTypeAction => ({
  type: 'UPDATE_THRESHOLDS_LIST_TYPE',
  payload: {
    thresholdsListType,
  },
})

export const updateGaugeColors = (
  gaugeColors: ColorNumber[]
): CellEditorOverlayActions.UpdateGaugeColorsAction => ({
  type: 'UPDATE_GAUGE_COLORS',
  payload: {
    gaugeColors,
  },
})

export const updateAxes = (
  axes: DashboardData.Axes
): CellEditorOverlayActions.UpdateAxesAction => ({
  type: 'UPDATE_AXES',
  payload: {
    axes,
  },
})

export const updateTableOptions = (
  tableOptions: DashboardData.TableOptions
): CellEditorOverlayActions.UpdateTableOptionsAction => ({
  type: 'UPDATE_TABLE_OPTIONS',
  payload: {
    tableOptions,
  },
})

export const updateLineColors = (
  lineColors: ColorString[]
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
  decimalPlaces: DashboardData.DecimalPlaces
): CellEditorOverlayActions.ChangeDecimalPlacesAction => ({
  type: 'CHANGE_DECIMAL_PLACES',
  payload: {
    decimalPlaces,
  },
})

export const updateFieldOptions = (
  fieldOptions: DashboardData.FieldOption[]
): CellEditorOverlayActions.UpdateFieldOptionsAction => ({
  type: 'UPDATE_FIELD_OPTIONS',
  payload: {
    fieldOptions,
  },
})
