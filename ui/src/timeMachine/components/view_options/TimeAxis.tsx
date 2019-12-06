// Libraries
import React, {SFC} from 'react'

// Components
import {Form, SelectGroup, Grid} from '@influxdata/clockface'

// Types
import {ButtonShape} from '@influxdata/clockface'

interface Props {
  verticalTimeAxis: boolean
  onToggleVerticalTimeAxis: (vertical: boolean) => void
}

const TimeAxis: SFC<Props> = ({verticalTimeAxis, onToggleVerticalTimeAxis}) => (
  <Grid.Column>
    <Form.Element label="Time Axis">
      <SelectGroup shape={ButtonShape.StretchToFit}>
        <SelectGroup.Option
          name="graph-time-axis"
          id="graph-time-axis--vertical"
          value={true}
          active={verticalTimeAxis}
          onClick={onToggleVerticalTimeAxis}
          titleText="Position time on the vertical table axis"
        >
          Vertical
        </SelectGroup.Option>
        <SelectGroup.Option
          name="graph-time-axis"
          id="graph-time-axis--horizontal"
          value={false}
          active={!verticalTimeAxis}
          onClick={onToggleVerticalTimeAxis}
          titleText="Position time on the horizontal table axis"
        >
          Horizontal
        </SelectGroup.Option>
      </SelectGroup>
    </Form.Element>
  </Grid.Column>
)

export default TimeAxis
