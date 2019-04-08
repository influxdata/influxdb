// Libraries
import React, {SFC} from 'react'

// Components
import {Form, Radio, Grid} from '@influxdata/clockface'

// Types
import {ButtonShape} from '@influxdata/clockface'

interface Props {
  verticalTimeAxis: boolean
  onToggleVerticalTimeAxis: (vertical: boolean) => void
}

const TimeAxis: SFC<Props> = ({verticalTimeAxis, onToggleVerticalTimeAxis}) => (
  <Grid.Column>
    <Form.Element label="Time Axis">
      <Radio shape={ButtonShape.StretchToFit}>
        <Radio.Button
          id="graph-time-axis--vertical"
          value={true}
          active={verticalTimeAxis}
          onClick={onToggleVerticalTimeAxis}
          titleText="Position time on the vertical table axis"
        >
          Vertical
        </Radio.Button>
        <Radio.Button
          id="graph-time-axis--horizontal"
          value={false}
          active={!verticalTimeAxis}
          onClick={onToggleVerticalTimeAxis}
          titleText="Position time on the horizontal table axis"
        >
          Horizontal
        </Radio.Button>
      </Radio>
    </Form.Element>
  </Grid.Column>
)

export default TimeAxis
