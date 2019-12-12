// Libraries
import React, {SFC} from 'react'

// Components
import {
  Form,
  InputLabel,
  SlideToggle,
  FlexBox,
  Grid,
} from '@influxdata/clockface'

// Types
import {Columns, FlexDirection, ComponentSize} from '@influxdata/clockface'

interface Props {
  fixed: boolean
  onToggleFixFirstColumn: () => void
}

const GraphOptionsFixFirstColumn: SFC<Props> = ({
  fixed,
  onToggleFixFirstColumn,
}) => (
  <Grid.Column widthXS={Columns.Twelve}>
    <Form.Element label="First Column">
      <Form.Box>
        <FlexBox direction={FlexDirection.Row} margin={ComponentSize.Small}>
          <InputLabel>Scroll with table</InputLabel>
          <SlideToggle
            active={fixed}
            onChange={onToggleFixFirstColumn}
            size={ComponentSize.ExtraSmall}
          />
          <InputLabel>Fixed</InputLabel>
        </FlexBox>
      </Form.Box>
    </Form.Element>
  </Grid.Column>
)

export default GraphOptionsFixFirstColumn
