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
    }

    expect(actual.cell).to.equal(expected.cell)
    expect(actual.gaugeColors).to.equal(expected.gaugeColors)
    expect(actual.singleStatColors).to.equal(expected.singleStatColors)
    expect(actual.singleStatType).to.equal(expected.singleStatType)
  })

  it('should hide cell editor overlay', () => {
    const actual = reducer(initialState, hideCellEditorOverlay)
    const expected = null

    expect(actual.cell).to.equal(expected)
  })

  it('should change the cell editor visualization type', () => {
    const actual = reducer(initialState, changeCellType(defaultCellType))
    const expected = defaultCellType

    expect(actual.cell.type).to.equal(expected)
  })

  it('should change the name of the cell', () => {
    const actual = reducer(initialState, renameCell(defaultCellName))
    const expected = defaultCellName

    expect(actual.cell.name).to.equal(expected)
  })

  it('should update the cell single stat colors', () => {
    const actual = reducer(
      initialState,
      updateSingleStatColors(defaultSingleStatColors)
    )
    const expected = defaultSingleStatColors

    expect(actual.singleStatColors).to.equal(expected)
  })

  it('should toggle the single stat type', () => {
    const actual = reducer(
      initialState,
      updateSingleStatType(defaultSingleStatType)
    )
    const expected = defaultSingleStatType

    expect(actual.singleStatType).to.equal(expected)
  })

  it('should update the cell gauge colors', () => {
    const actual = reducer(initialState, updateGaugeColors(defaultGaugeColors))
    const expected = defaultGaugeColors

    expect(actual.gaugeColors).to.equal(expected)
  })

  it('should update the cell axes', () => {
    const actual = reducer(initialState, updateAxes(defaultCellAxes))
    const expected = defaultCellAxes

    expect(actual.cell.axes).to.equal(expected)
  })
})
