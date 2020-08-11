// Libraries
import React, {SFC} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {Form, Grid, Input} from '@influxdata/clockface'
import TimeFormat from 'src/timeMachine/components/view_options/TimeFormat'

// Actions
import {
  setFillColumns,
  setYAxisLabel,
  setXAxisLabel,
  setColorHexes,
  setYDomain,
  setXColumn,
  setYSeriesColumns,
  setTimeFormat,
} from 'src/timeMachine/actions'

// Utils
import {
  getGroupableColumns,
  getMosaicFillColumnsSelection,
  getXColumnSelection,
  getMosaicYColumnSelection,
  getNumericColumns,
  getStringColumns,
  getActiveTimeMachine,
} from 'src/timeMachine/selectors'

// Constants
import {GIRAFFE_COLOR_SCHEMES} from 'src/shared/constants'

// Types
import {AppState, NewView, MosaicViewProperties} from 'src/types'
import HexColorSchemeDropdown from 'src/shared/components/HexColorSchemeDropdown'
import AutoDomainInput from 'src/shared/components/AutoDomainInput'
import ColumnSelector from 'src/shared/components/ColumnSelector'

interface OwnProps {
  xColumn: string
  yColumn: string
  fillColumns: string
  xDomain: number[]
  yDomain: number[]
  xAxisLabel: string
  yAxisLabel: string
  colors: string[]
  showNoteWhenEmpty: boolean
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

const MosaicOptions: SFC<Props> = props => {
  const {
    fillColumns,
    yAxisLabel,
    xAxisLabel,
    onSetFillColumns,
    colors,
    onSetColors,
    onSetYAxisLabel,
    onSetXAxisLabel,
    yDomain,
    onSetYDomain,
    xColumn,
    yColumn,
    stringColumns,
    numericColumns,
    onSetXColumn,
    onSetYSeriesColumns,
    onSetTimeFormat,
    timeFormat,
  } = props

  const handleFillColumnSelect = (column: string): void => {
    const fillColumn = [column]
    onSetFillColumns(fillColumn)
  }

  const onSelectYSeriesColumns = (colName: string) => {
    onSetYSeriesColumns([colName])
  }

  return (
    <Grid.Column>
      <h4 className="view-options--header">Customize Mosaic Plot</h4>
      <h5 className="view-options--header">Data</h5>
      <ColumnSelector
        selectedColumn={fillColumns[0]}
        onSelectColumn={handleFillColumnSelect}
        availableColumns={stringColumns}
        axisName="fill"
      />
      <ColumnSelector
        selectedColumn={xColumn}
        onSelectColumn={onSetXColumn}
        availableColumns={numericColumns}
        axisName="x"
      />
      <ColumnSelector
        selectedColumn={yColumn}
        onSelectColumn={onSelectYSeriesColumns}
        availableColumns={stringColumns}
        axisName="y"
      />
      <Form.Element label="Time Format">
        <TimeFormat
          timeFormat={timeFormat}
          onTimeFormatChange={onSetTimeFormat}
        />
      </Form.Element>
      <h5 className="view-options--header">Options</h5>
      <Form.Element label="Color Scheme">
        <HexColorSchemeDropdown
          colorSchemes={GIRAFFE_COLOR_SCHEMES}
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
      </Form.Element>{' '}
      <AutoDomainInput
        domain={yDomain as [number, number]}
        onSetDomain={onSetYDomain}
        label="Y Axis Domain"
      />
    </Grid.Column>
  )
}

const mstp = (state: AppState) => {
  const availableGroupColumns = getGroupableColumns(state)
  const fillColumns = getMosaicFillColumnsSelection(state)
  const xColumn = getXColumnSelection(state)
  const yColumn = getMosaicYColumnSelection(state)
  const stringColumns = getStringColumns(state)
  const numericColumns = getNumericColumns(state)
  const view = getActiveTimeMachine(state).view as NewView<MosaicViewProperties>
  const {timeFormat} = view.properties

  return {
    availableGroupColumns,
    fillColumns,
    xColumn,
    yColumn,
    stringColumns,
    numericColumns,
    timeFormat,
  }
}

const mdtp = {
  onSetFillColumns: setFillColumns,
  onSetColors: setColorHexes,
  onSetYAxisLabel: setYAxisLabel,
  onSetXAxisLabel: setXAxisLabel,
  onSetYDomain: setYDomain,
  onSetXColumn: setXColumn,
  onSetYSeriesColumns: setYSeriesColumns,
  onSetTimeFormat: setTimeFormat,
}

const connector = connect(mstp, mdtp)
export default connector(MosaicOptions)
