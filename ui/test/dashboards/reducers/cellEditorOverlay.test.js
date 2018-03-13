import reducer, {initialState} from 'src/dashboards/reducers/cellEditorOverlay'

import {
  showCellEditorOverlay,
  hideCellEditorOverlay,
  changeCellType,
  renameCell,
  updateSingleStatColors,
  updateSingleStatType,
  updateGaugeColors,
  updateAxes,
} from 'src/dashboards/actions/cellEditorOverlay'
import {DEFAULT_TABLE_OPTIONS} from 'src/shared/constants/TableGraph'

import {
  validateGaugeColors,
  validateSingleStatColors,
  getSingleStatType,
} from 'src/dashboards/constants/gaugeColors'

const defaultCellType = 'line'
const defaultCellName = 'defaultCell'
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

const defaultSingleStatType = getSingleStatType(defaultCell.colors)
const defaultSingleStatColors = validateSingleStatColors(
  defaultCell.colors,
  defaultSingleStatType
)
const defaultGaugeColors = validateGaugeColors(defaultCell.colors)

describe('Dashboards.Reducers.cellEditorOverlay', () => {
  it('should show cell editor overlay', () => {
    const actual = reducer(initialState, showCellEditorOverlay(defaultCell))
    const expected = {
      cell: defaultCell,
      gaugeColors: defaultGaugeColors,
      singleStatColors: defaultSingleStatColors,
      singleStatType: defaultSingleStatType,
      tableOptions: DEFAULT_TABLE_OPTIONS,
    }

    expect(actual.cell).toEqual(expected.cell)
    expect(actual.gaugeColors).toBe(expected.gaugeColors)
    expect(actual.singleStatColors).toBe(expected.singleStatColors)
    expect(actual.singleStatType).toBe(expected.singleStatType)
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
      updateSingleStatColors(defaultSingleStatColors)
    )
    const expected = defaultSingleStatColors

    expect(actual.singleStatColors).toBe(expected)
  })

  it('should toggle the single stat type', () => {
    const actual = reducer(
      initialState,
      updateSingleStatType(defaultSingleStatType)
    )
    const expected = defaultSingleStatType

    expect(actual.singleStatType).toBe(expected)
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
})
