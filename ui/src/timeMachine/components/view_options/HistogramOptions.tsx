// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {
  Form,
  Input,
  Grid,
  Dropdown,
  MultiSelectDropdown,
} from '@influxdata/clockface'
import ColorSchemeDropdown from 'src/shared/components/ColorSchemeDropdown'
import AutoDomainInput from 'src/shared/components/AutoDomainInput'
import BinCountInput from 'src/timeMachine/components/view_options/BinCountInput'

// Actions
import {
  setXColumn,
  setFillColumns,
  setBinCount,
  setHistogramPosition,
  setColors,
  setXDomain,
  setXAxisLabel,
} from 'src/timeMachine/actions'

// Utils
import {
  getXColumnSelection,
  getNumericColumns,
  getGroupableColumns,
  getFillColumnsSelection,
} from 'src/timeMachine/selectors'

// Types
import {ComponentStatus} from '@influxdata/clockface'
import {HistogramPosition} from '@influxdata/giraffe'
import {Color} from 'src/types/colors'
import {AppState} from 'src/types'
import ColumnSelector from 'src/shared/components/ColumnSelector'

interface StateProps {
  xColumn: string
  fillColumns: string[]
  numericColumns: string[]
  availableGroupColumns: string[]
}

interface DispatchProps {
  onSetXColumn: typeof setXColumn
  onSetFillColumns: typeof setFillColumns
  onSetBinCount: typeof setBinCount
  onSetPosition: typeof setHistogramPosition
  onSetColors: typeof setColors
  onSetXDomain: typeof setXDomain
  onSetXAxisLabel: typeof setXAxisLabel
}

interface OwnProps {
  position: HistogramPosition
  binCount: number
  colors: Color[]
  xDomain: number[]
  xAxisLabel: string
}

type Props = OwnProps & DispatchProps & StateProps

const HistogramOptions: SFC<Props> = props => {
  const {
    xColumn,
    fillColumns,
    numericColumns,
    availableGroupColumns,
    position,
    binCount,
    colors,
    xDomain,
    xAxisLabel,
    onSetXColumn,
    onSetFillColumns,
    onSetPosition,
    onSetBinCount,
    onSetColors,
    onSetXDomain,
    onSetXAxisLabel,
  } = props

  const groupDropdownStatus = availableGroupColumns.length
    ? ComponentStatus.Default
    : ComponentStatus.Disabled

  const onSelectFillColumns = (option: string) => {
    const columnExists = fillColumns.find(col => col === option)
    let updatedColumns = fillColumns

    if (columnExists) {
      updatedColumns = fillColumns.filter(fc => fc !== option)
    } else {
      updatedColumns = [...fillColumns, option]
    }

    onSetFillColumns(updatedColumns)
  }

  return (
    <Grid.Column>
      <h4 className="view-options--header">Customize Histogram</h4>
      <h5 className="view-options--header">Data</h5>
      <ColumnSelector
        selectedColumn={xColumn}
        onSelectColumn={onSetXColumn}
        availableColumns={numericColumns}
        axisName="x"
      />
      <Form.Element label="Group By">
        <MultiSelectDropdown
          options={availableGroupColumns}
          selectedOptions={fillColumns}
          onSelect={onSelectFillColumns}
          buttonStatus={groupDropdownStatus}
        />
      </Form.Element>
      <h5 className="view-options--header">Options</h5>
      <Form.Element label="Color Scheme">
        <ColorSchemeDropdown value={colors} onChange={onSetColors} />
      </Form.Element>
      <Form.Element label="Positioning">
        <Dropdown
          button={(active, onClick) => (
            <Dropdown.Button active={active} onClick={onClick}>
              {_.capitalize(position)}
            </Dropdown.Button>
          )}
          menu={onCollapse => (
            <Dropdown.Menu onCollapse={onCollapse}>
              <Dropdown.Item
                id="overlaid"
                value="overlaid"
                onClick={onSetPosition}
                selected={position === 'overlaid'}
              >
                Overlaid
              </Dropdown.Item>
              <Dropdown.Item
                id="stacked"
                value="stacked"
                onClick={onSetPosition}
                selected={position === 'stacked'}
              >
                Stacked
              </Dropdown.Item>
            </Dropdown.Menu>
          )}
        />
      </Form.Element>
      <Form.Element label="Bins">
        <BinCountInput binCount={binCount} onSetBinCount={onSetBinCount} />
      </Form.Element>
      <h5 className="view-options--header">X Axis</h5>
      <Form.Element label="X Axis Label">
        <Input
          value={xAxisLabel}
          onChange={e => onSetXAxisLabel(e.target.value)}
        />
      </Form.Element>
      <AutoDomainInput
        domain={xDomain as [number, number]}
        onSetDomain={onSetXDomain}
        label="X Axis Domain"
      />
    </Grid.Column>
  )
}

const mstp = (state: AppState) => {
  const numericColumns = getNumericColumns(state)
  const availableGroupColumns = getGroupableColumns(state)
  const xColumn = getXColumnSelection(state)
  const fillColumns = getFillColumnsSelection(state)

  return {numericColumns, availableGroupColumns, xColumn, fillColumns}
}

const mdtp = {
  onSetXColumn: setXColumn,
  onSetFillColumns: setFillColumns,
  onSetBinCount: setBinCount,
  onSetPosition: setHistogramPosition,
  onSetColors: setColors,
  onSetXDomain: setXDomain,
  onSetXAxisLabel: setXAxisLabel,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(HistogramOptions)
