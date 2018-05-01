import reducer, {initialState} from 'src/dashboards/reducers/cellEditorOverlay'

import {
  showCellEditorOverlay,
  hideCellEditorOverlay,
  changeCellType,
  renameCell,
  updateThresholdsListColors,
  updateThresholdsListType,
  updateGaugeColors,
  updateLineColors,
  updateAxes,
} from 'src/dashboards/actions/cellEditorOverlay'
import {DEFAULT_TABLE_OPTIONS} from 'src/dashboards/constants'

import {
  validateGaugeColors,
  validateThresholdsListColors,
  getThresholdsListType,
} from 'shared/constants/thresholds'
import {validateLineColors} from 'src/shared/constants/graphColorPalettes'

import {CELL_TYPE_LINE} from 'src/dashboards/graphics/graph'
import {UNTITLED_CELL_LINE} from 'src/dashboards/constants'

const defaultCellType = CELL_TYPE_LINE
const defaultCellName = UNTITLED_CELL_LINE
const defaultCellAxes = {
  y: {
    base: '10',
    bounds: ['0', ''],
    label: '',
    prefix: '',
    scale: 'linear',
    suffix: '',
  },
}

const defaultCell = {
  axes: defaultCellAxes,
  colors: [],
  name: defaultCellName,
  type: defaultCellType,
  tableOptions: DEFAULT_TABLE_OPTIONS,
}

const defaultThresholdsListType = getThresholdsListType(defaultCell.colors)
const defaultThresholdsListColors = validateThresholdsListColors(
  defaultCell.colors,
  defaultThresholdsListType
)
const defaultGaugeColors = validateGaugeColors(defaultCell.colors)
const defaultLineColors = validateLineColors(defaultCell.colors)

describe('Dashboards.Reducers.cellEditorOverlay', () => {
  it('should show cell editor overlay', () => {
    const actual = reducer(initialState, showCellEditorOverlay(defaultCell))
    const expected = {
      cell: defaultCell,
      gaugeColors: defaultGaugeColors,
      thresholdsListColors: defaultThresholdsListColors,
      thresholdsListType: defaultThresholdsListType,
      tableOptions: DEFAULT_TABLE_OPTIONS,
    }

    expect(actual.cell).toEqual(expected.cell)
    expect(actual.gaugeColors).toBe(expected.gaugeColors)
    expect(actual.thresholdsListColors).toBe(expected.thresholdsListColors)
    expect(actual.thresholdsListType).toBe(expected.thresholdsListType)
  })

  it('should hide cell editor overlay', () => {
    const actual = reducer(initialState, hideCellEditorOverlay)
    const expected = null

    expect(actual.cell).toBe(expected)
  })

  it('should change the cell editor visualization type', () => {
    const actual = reducer(initialState, changeCellType(defaultCellType))
    const expected = defaultCellType

    expect(actual.cell.type).toBe(expected)
  })

  it('should change the name of the cell', () => {
    const actual = reducer(initialState, renameCell(defaultCellName))
    const expected = defaultCellName

    expect(actual.cell.name).toBe(expected)
  })

  it('should update the cell single stat colors', () => {
    const actual = reducer(
      initialState,
      updateThresholdsListColors(defaultThresholdsListColors)
    )
    const expected = defaultThresholdsListColors

    expect(actual.thresholdsListColors).toBe(expected)
  })

  it('should toggle the single stat type', () => {
    const actual = reducer(
      initialState,
      updateThresholdsListType(defaultThresholdsListType)
    )
    const expected = defaultThresholdsListType

    expect(actual.thresholdsListType).toBe(expected)
  })

  it('should update the cell gauge colors', () => {
    const actual = reducer(initialState, updateGaugeColors(defaultGaugeColors))
    const expected = defaultGaugeColors

    expect(actual.gaugeColors).toBe(expected)
  })

  it('should update the cell axes', () => {
    const actual = reducer(initialState, updateAxes(defaultCellAxes))
    const expected = defaultCellAxes

    expect(actual.cell.axes).toBe(expected)
  })

  it('should update the cell line graph colors', () => {
    const actual = reducer(initialState, updateLineColors(defaultLineColors))
    const expected = defaultLineColors

    expect(actual.lineColors).toBe(expected)
  })
})
