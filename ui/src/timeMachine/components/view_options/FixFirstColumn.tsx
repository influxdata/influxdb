import React, {SFC} from 'react'

import {
  SlideToggle,
  Grid,
  Form,
  Columns,
  ComponentSize,
  ComponentSpacer,
  Stack,
  Alignment,
} from 'src/clockface'

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
        <ComponentSpacer stackChildren={Stack.Columns} align={Alignment.Left}>
          <SlideToggle.Label text="Scroll with table" />
          <SlideToggle
            active={fixed}
            onChange={onToggleFixFirstColumn}
            size={ComponentSize.ExtraSmall}
          />
          <SlideToggle.Label text="Fixed" />
        </ComponentSpacer>
      </Form.Box>
    </Form.Element>
  </Grid.Column>
)

export default GraphOptionsFixFirstColumn
