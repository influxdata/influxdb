// Libraries
import React, {FunctionComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import {VIRIDIS, MAGMA, INFERNO, PLASMA} from '@influxdata/vis'
import {Form, Grid, Input, Columns, InputType} from '@influxdata/clockface'

// Components
import AutoDomainInput from 'src/shared/components/AutoDomainInput'
import HexColorSchemeDropdown from 'src/shared/components/HexColorSchemeDropdown'
import ColumnSelector from 'src/shared/components/ColumnSelector'

// Actions
import {
  setXColumn,
  setYColumn,
  setBinSize,
  setColorHexes,
  setXDomain,
  setYDomain,
  setXAxisLabel,
  setYAxisLabel,
  setAxisPrefix,
  setAxisSuffix,
} from 'src/timeMachine/actions'

// Utils
import {
  getXColumnSelection,
  getYColumnSelection,
  getNumericColumns,
} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types'

const HEATMAP_COLOR_SCHEMES = [
  {name: 'Magma', colors: MAGMA},
  {name: 'Inferno', colors: INFERNO},
  {name: 'Viridis', colors: VIRIDIS},
  {name: 'Plasma', colors: PLASMA},
]

interface StateProps {
  xColumn: string
  yColumn: string
  numericColumns: string[]
}

interface DispatchProps {
  onSetXColumn: typeof setXColumn
  onSetYColumn: typeof setYColumn
  onSetBinSize: typeof setBinSize
  onSetColors: typeof setColorHexes
  onSetXDomain: typeof setXDomain
  onSetYDomain: typeof setYDomain
  onSetXAxisLabel: typeof setXAxisLabel
  onSetYAxisLabel: typeof setYAxisLabel
  onSetPrefix: typeof setAxisPrefix
  onSetSuffix: typeof setAxisSuffix
}

interface OwnProps {
  xDomain: [number, number]
  yDomain: [number, number]
  xAxisLabel: string
  yAxisLabel: string
  xPrefix: string
  xSuffix: string
  yPrefix: string
  ySuffix: string
  colors: string[]
  binSize: number
}

type Props = StateProps & DispatchProps & OwnProps

const HeatmapOptions: FunctionComponent<Props> = props => {
  const onSetBinSize = (e: ChangeEvent<HTMLInputElement>) => {
    const val = +e.target.value

    if (isNaN(val) || val < 5) {
      return
    }

    props.onSetBinSize(val)
  }

  return (
    <Grid.Column>
      <h4 className="view-options--header">Customize Heatmap</h4>
      <h5 className="view-options--header">Data</h5>
      <ColumnSelector
        selectedColumn={props.xColumn}
        onSelectColumn={props.onSetXColumn}
        availableColumns={props.numericColumns}
        axisName="x"
      />
      <ColumnSelector
        selectedColumn={props.yColumn}
        onSelectColumn={props.onSetYColumn}
        availableColumns={props.numericColumns}
        axisName="y"
      />

      <h5 className="view-options--header">Options</h5>
      <Form.Element label="Color Scheme">
        <HexColorSchemeDropdown
          colorSchemes={HEATMAP_COLOR_SCHEMES}
          selectedColorScheme={props.colors}
          onSelectColorScheme={props.onSetColors}
        />
      </Form.Element>
      <Form.Element label="Bin Size">
        <Input
          value={props.binSize}
          type={InputType.Number}
          onChange={onSetBinSize}
        />
      </Form.Element>
      <h5 className="view-options--header">X Axis</h5>
      <Form.Element label="X Axis Label">
        <Input
          value={props.xAxisLabel}
          onChange={e => props.onSetXAxisLabel(e.target.value)}
        />
      </Form.Element>
      <Grid.Row>
        <Grid.Column widthSM={Columns.Six}>
          <Form.Element label="X Tick Prefix">
            <Input
              value={props.xPrefix}
              onChange={e => props.onSetPrefix(e.target.value, 'x')}
            />
          </Form.Element>
        </Grid.Column>
        <Grid.Column widthSM={Columns.Six}>
          <Form.Element label="X Tick Suffix">
            <Input
              value={props.xSuffix}
              onChange={e => props.onSetSuffix(e.target.value, 'x')}
            />
          </Form.Element>
        </Grid.Column>
      </Grid.Row>
      <AutoDomainInput
        domain={props.xDomain}
        onSetDomain={props.onSetXDomain}
        label="X Axis Domain"
      />
      <h5 className="view-options--header">Y Axis</h5>
      <Form.Element label="Y Axis Label">
        <Input
          value={props.yAxisLabel}
          onChange={e => props.onSetYAxisLabel(e.target.value)}
        />
      </Form.Element>
      <Grid.Row>
        <Grid.Column widthSM={Columns.Six}>
          <Form.Element label="Y Tick Prefix">
            <Input
              value={props.yPrefix}
              onChange={e => props.onSetPrefix(e.target.value, 'y')}
            />
          </Form.Element>
        </Grid.Column>
        <Grid.Column widthSM={Columns.Six}>
          <Form.Element label="Y Tick Suffix">
            <Input
              value={props.ySuffix}
              onChange={e => props.onSetSuffix(e.target.value, 'y')}
            />
          </Form.Element>
        </Grid.Column>
      </Grid.Row>
      <AutoDomainInput
        domain={props.yDomain}
        onSetDomain={props.onSetYDomain}
        label="Y Axis Domain"
      />
    </Grid.Column>
  )
}

const mstp = (state: AppState) => {
  const xColumn = getXColumnSelection(state)
  const yColumn = getYColumnSelection(state)
  const numericColumns = getNumericColumns(state)

  return {xColumn, yColumn, numericColumns}
}

const mdtp = {
  onSetXColumn: setXColumn,
  onSetYColumn: setYColumn,
  onSetBinSize: setBinSize,
  onSetColors: setColorHexes,
  onSetXDomain: setXDomain,
  onSetYDomain: setYDomain,
  onSetXAxisLabel: setXAxisLabel,
  onSetYAxisLabel: setYAxisLabel,
  onSetPrefix: setAxisPrefix,
  onSetSuffix: setAxisSuffix,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(HeatmapOptions)
