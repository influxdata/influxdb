// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Dropdown,
  MultiSelectDropdown,
  ComponentStatus,
  Form,
  Grid,
  AutoInput,
} from 'src/clockface'
import ColorSchemeDropdown from 'src/shared/components/ColorSchemeDropdown'

// Actions
import {
  setXColumn,
  setFillColumns,
  setBinCount,
  setHistogramPosition,
  setColors,
} from 'src/timeMachine/actions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Styles
import 'src/timeMachine/components/view_options/HistogramOptions.scss'

// Types
import {HistogramPosition} from 'src/minard'
import {Color} from 'src/types/colors'
import {AppState} from 'src/types/v2'

interface StateProps {
  availableXColumns: string[]
  availableGroupColumns: string[]
}

interface DispatchProps {
  onSetXColumn: typeof setXColumn
  onSetFillColumns: typeof setFillColumns
  onSetBinCount: typeof setBinCount
  onSetPosition: typeof setHistogramPosition
  onSetColors: typeof setColors
}

interface OwnProps {
  xColumn: string
  fillColumns: string[]
  position: HistogramPosition
  binCount: number
  colors: Color[]
}

type Props = OwnProps & DispatchProps & StateProps

const HistogramOptions: SFC<Props> = props => {
  const {
    xColumn,
    fillColumns,
    availableXColumns,
    availableGroupColumns,
    position,
    binCount,
    colors,
    onSetXColumn,
    onSetFillColumns,
    onSetPosition,
    onSetBinCount,
    onSetColors,
  } = props

  const xDropdownStatus = availableXColumns.length
    ? ComponentStatus.Default
    : ComponentStatus.Disabled

  const groupDropdownStatus = availableGroupColumns.length
    ? ComponentStatus.Default
    : ComponentStatus.Disabled

  return (
    <Grid.Column>
      <h4 className="view-options--header">Customize Histogram</h4>
      <h5 className="view-options--header">Data</h5>
      <Form.Element label="Column">
        <Dropdown
          selectedID={xColumn}
          onChange={onSetXColumn}
          status={xDropdownStatus}
          titleText="None"
        >
          {availableXColumns.map(columnName => (
            <Dropdown.Item id={columnName} key={columnName} value={columnName}>
              {columnName}
            </Dropdown.Item>
          ))}
        </Dropdown>
      </Form.Element>
      <Form.Element label="Group By">
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
      <h5 className="view-options--header">Options</h5>
      <Form.Element label="Color Scheme">
        <ColorSchemeDropdown value={colors} onChange={onSetColors} />
      </Form.Element>
      <Form.Element label="Positioning">
        <Dropdown selectedID={position} onChange={onSetPosition}>
          <Dropdown.Item
            id={HistogramPosition.Overlaid}
            value={HistogramPosition.Overlaid}
          >
            Overlaid
          </Dropdown.Item>
          <Dropdown.Item
            id={HistogramPosition.Stacked}
            value={HistogramPosition.Stacked}
          >
            Stacked
          </Dropdown.Item>
        </Dropdown>
      </Form.Element>
      <Form.Element label="Bins">
        <AutoInput
          name="binCount"
          inputPlaceholder="Enter a number"
          value={binCount}
          onChange={onSetBinCount}
          min={0}
        />
      </Form.Element>
    </Grid.Column>
  )
}

const mstp = (state: AppState) => {
  const {availableXColumns, availableGroupColumns} = getActiveTimeMachine(state)

  return {availableXColumns, availableGroupColumns}
}

const mdtp = {
  onSetXColumn: setXColumn,
  onSetFillColumns: setFillColumns,
  onSetBinCount: setBinCount,
  onSetPosition: setHistogramPosition,
  onSetColors: setColors,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(HistogramOptions)
