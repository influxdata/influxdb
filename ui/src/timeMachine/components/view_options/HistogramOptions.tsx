// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'

// Components
import {Dropdown, Form, Grid, AutoInput} from 'src/clockface'
import ColorSchemeDropdown from 'src/shared/components/ColorSchemeDropdown'

// Actions
import {
  setX,
  setFill,
  setBinCount,
  setHistogramPosition,
  setColors,
} from 'src/timeMachine/actions'

// Styles
import 'src/timeMachine/components/view_options/HistogramOptions.scss'

// Types
import {HistogramPosition} from 'src/minard'
import {Color} from 'src/types/colors'

interface DispatchProps {
  onSetX: typeof setX
  onSetFill: typeof setFill
  onSetBinCount: typeof setBinCount
  onSetPosition: typeof setHistogramPosition
  onSetColors: typeof setColors
}

interface OwnProps {
  x: string
  fill: string
  position: HistogramPosition
  binCount: number
  colors: Color[]
}

type Props = OwnProps & DispatchProps

const NO_FILL = 'None'

// TODO: These options are currently hardcoded
const HistogramOptions: SFC<Props> = props => {
  const {
    x,
    fill,
    position,
    binCount,
    colors,
    onSetX,
    onSetFill,
    onSetPosition,
    onSetBinCount,
    onSetColors,
  } = props

  return (
    <Grid.Column>
      <h4 className="view-options--header">Customize Histogram</h4>
      <h5 className="view-options--header">Data</h5>
      <Form.Element label="Column">
        <Dropdown selectedID={x} onChange={onSetX}>
          <Dropdown.Item id="_value" value="_value">
            <div className="column-option">_value</div>
          </Dropdown.Item>
          <Dropdown.Item id="_time" value="_time">
            <div className="column-option">_time</div>
          </Dropdown.Item>
        </Dropdown>
      </Form.Element>
      <Form.Element label="Fill">
        <Dropdown selectedID={fill ? fill : NO_FILL} onChange={onSetFill}>
          <Dropdown.Item id={NO_FILL} value={null}>
            None
          </Dropdown.Item>
          <Dropdown.Item id="table" value="table">
            Flux Group Key
          </Dropdown.Item>
          <Dropdown.Item id="cpu" value="cpu">
            <div className="column-option">cpu</div>
          </Dropdown.Item>
          <Dropdown.Item id="host" value="host">
            <div className="column-option">host</div>
          </Dropdown.Item>
          <Dropdown.Item id="_measurement" value="_measurement">
            <div className="column-option">_measurement</div>
          </Dropdown.Item>
          <Dropdown.Item id="_field" value="_field">
            <div className="column-option">_field</div>
          </Dropdown.Item>
        </Dropdown>
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

const mdtp = {
  onSetX: setX,
  onSetFill: setFill,
  onSetBinCount: setBinCount,
  onSetPosition: setHistogramPosition,
  onSetColors: setColors,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(HistogramOptions)
