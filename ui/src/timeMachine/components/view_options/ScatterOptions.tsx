// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'
import {
  NINETEEN_EIGHTY_FOUR,
  ATLANTIS,
  DO_ANDROIDS_DREAM,
  DELOREAN,
  CTHULHU,
  ECTOPLASM,
  T_MAX_400_FILM,
} from '@influxdata/vis'

// Components
import {Form, Input, Grid} from '@influxdata/clockface'
import {Dropdown, MultiSelectDropdown} from 'src/clockface'
import AxisAffixes from 'src/timeMachine/components/view_options/AxisAffixes'

// Actions
import {
  setFillColumns,
  setSymbolColumns,
  setYAxisLabel,
  setXAxisLabel,
  setAxisPrefix,
  setAxisSuffix,
  setColorHexes,
  setYDomain,
  setXColumn,
  setYColumn,
} from 'src/timeMachine/actions'

// Utils
import {
  getGroupableColumns,
  getFillColumnsSelection,
  getSymbolColumnsSelection,
  getXColumnSelection,
  getYColumnSelection,
  getNumericColumns,
} from 'src/timeMachine/selectors'

// Types
import {ComponentStatus} from '@influxdata/clockface'
import {AppState} from 'src/types'
import HexColorSchemeDropdown from 'src/shared/components/HexColorSchemeDropdown'
import AutoDomainInput from 'src/shared/components/AutoDomainInput'
import ColumnSelector from 'src/shared/components/ColumnSelector'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'

const COLOR_SCHEMES = [
  {name: 'Nineteen Eighty Four', colors: NINETEEN_EIGHTY_FOUR},
  {name: 'Atlantis', colors: ATLANTIS},
  {name: 'Do Androids Dream of Electric Sheep?', colors: DO_ANDROIDS_DREAM},
  {name: 'Delorean', colors: DELOREAN},
  {name: 'Cthulhu', colors: CTHULHU},
  {name: 'Ectoplasm', colors: ECTOPLASM},
  {name: 'T-MAX 400 Film', colors: T_MAX_400_FILM},
]

interface StateProps {
  fillColumns: string[]
  symbolColumns: string[]
  availableGroupColumns: string[]
  xColumn: string
  yColumn: string
  numericColumns: string[]
}

interface DispatchProps {
  onSetFillColumns: typeof setFillColumns
  onSetSymbolColumns: typeof setSymbolColumns
  onSetColors: typeof setColorHexes
  onSetYAxisLabel: typeof setYAxisLabel
  onSetXAxisLabel: typeof setXAxisLabel
  onUpdateAxisSuffix: typeof setAxisSuffix
  onUpdateAxisPrefix: typeof setAxisPrefix
  onSetYDomain: typeof setYDomain
  onSetXColumn: typeof setXColumn
  onSetYColumn: typeof setYColumn
}

interface OwnProps {
  xColumn: string
  yColumn: string
  fillColumns: string[]
  symbolColumns: string[]
  xDomain: [number, number]
  yDomain: [number, number]
  xAxisLabel: string
  yAxisLabel: string
  xPrefix: string
  xSuffix: string
  yPrefix: string
  ySuffix: string
  colors: string[]
  showNoteWhenEmpty: boolean
}

type Props = OwnProps & DispatchProps & StateProps

const ScatterOptions: SFC<Props> = props => {
  const {
    fillColumns,
    symbolColumns,
    availableGroupColumns,
    yAxisLabel,
    xAxisLabel,
    onSetFillColumns,
    onSetSymbolColumns,
    colors,
    onSetColors,
    onSetYAxisLabel,
    onSetXAxisLabel,
    yPrefix,
    ySuffix,
    onUpdateAxisSuffix,
    onUpdateAxisPrefix,
    yDomain,
    onSetYDomain,
    xColumn,
    yColumn,
    numericColumns,
    onSetXColumn,
    onSetYColumn,
  } = props

  const groupDropdownStatus = availableGroupColumns.length
    ? ComponentStatus.Default
    : ComponentStatus.Disabled

  return (
    <Grid.Column>
      <h4 className="view-options--header">Customize Scatter Plot</h4>
      <h5 className="view-options--header">Data</h5>

      <Form.Element label="Symbol Column">
        <MultiSelectDropdown
          selectedIDs={symbolColumns}
          onChange={onSetSymbolColumns}
          status={groupDropdownStatus}
        >
          {availableGroupColumns.map(columnName => (
            <Dropdown.Item
              id={columnName}
              key={columnName}
              value={{id: columnName}}
            >
              {columnName}
            </Dropdown.Item>
          ))}
        </MultiSelectDropdown>
      </Form.Element>
      <Form.Element label="Fill Column">
        <MultiSelectDropdown
          selectedIDs={fillColumns}
          onChange={onSetFillColumns}
          status={groupDropdownStatus}
        >
          {availableGroupColumns.map(columnName => (
            <Dropdown.Item
              id={columnName}
              key={columnName}
              value={{id: columnName}}
            >
              {columnName}
            </Dropdown.Item>
          ))}
        </MultiSelectDropdown>
      </Form.Element>
      <CloudExclude>
        <ColumnSelector
          selectedColumn={xColumn}
          onSelectColumn={onSetXColumn}
          availableColumns={numericColumns}
          axisName="x"
        />
        <ColumnSelector
          selectedColumn={yColumn}
          onSelectColumn={onSetYColumn}
          availableColumns={numericColumns}
          axisName="y"
        />
      </CloudExclude>
      <h5 className="view-options--header">Options</h5>
      <Form.Element label="Color Scheme">
        <HexColorSchemeDropdown
          colorSchemes={COLOR_SCHEMES}
          selectedColorScheme={colors}
          onSelectColorScheme={onSetColors}
        />
      </Form.Element>
      <h5 className="view-options--header">X Axis</h5>
      <Form.Element label="X Axis Label">
        <Input
          value={xAxisLabel}
          onChange={e => onSetXAxisLabel(e.target.value)}
        />
      </Form.Element>
      <h5 className="view-options--header">Y Axis</h5>
      <Form.Element label="Y Axis Label">
        <Input
          value={yAxisLabel}
          onChange={e => onSetYAxisLabel(e.target.value)}
        />
      </Form.Element>
      <Grid.Row>
        <AxisAffixes
          prefix={yPrefix}
          suffix={ySuffix}
          axisName="y"
          onUpdateAxisPrefix={prefix => onUpdateAxisPrefix(prefix, 'y')}
          onUpdateAxisSuffix={suffix => onUpdateAxisSuffix(suffix, 'y')}
        />
      </Grid.Row>
      <AutoDomainInput
        domain={yDomain}
        onSetDomain={onSetYDomain}
        label="Y Axis Domain"
      />
    </Grid.Column>
  )
}

const mstp = (state: AppState): StateProps => {
  const availableGroupColumns = getGroupableColumns(state)
  const fillColumns = getFillColumnsSelection(state)
  const symbolColumns = getSymbolColumnsSelection(state)
  const xColumn = getXColumnSelection(state)
  const yColumn = getYColumnSelection(state)
  const numericColumns = getNumericColumns(state)

  return {
    availableGroupColumns,
    fillColumns,
    symbolColumns,
    xColumn,
    yColumn,
    numericColumns,
  }
}

const mdtp = {
  onSetFillColumns: setFillColumns,
  onSetSymbolColumns: setSymbolColumns,
  onSetColors: setColorHexes,
  onSetYAxisLabel: setYAxisLabel,
  onSetXAxisLabel: setXAxisLabel,
  onUpdateAxisPrefix: setAxisPrefix,
  onUpdateAxisSuffix: setAxisSuffix,
  onSetYDomain: setYDomain,
  onSetXColumn: setXColumn,
  onSetYColumn: setYColumn,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(ScatterOptions)
