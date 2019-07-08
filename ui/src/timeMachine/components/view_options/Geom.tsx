// Libraries
import React, {SFC} from 'react'

// Components
import {Form, Grid} from '@influxdata/clockface'
import {Dropdown} from 'src/clockface'

// Utils
import {resolveGeom} from 'src/shared/utils/vis'

// Types
import {XYViewGeom} from 'src/types'
import {Columns} from '@influxdata/clockface'

interface Props {
  geom: XYViewGeom
  onSetGeom: (geom: XYViewGeom) => void
}

const Geom: SFC<Props> = ({geom, onSetGeom}) => {
  return (
    <Grid.Column widthXS={Columns.Twelve}>
      <Form.Element label="Interpolation">
        <Dropdown selectedID={resolveGeom(geom)} onChange={onSetGeom}>
          <Dropdown.Item id={XYViewGeom.Line} value={XYViewGeom.Line}>
            Linear
          </Dropdown.Item>
          <Dropdown.Item id={XYViewGeom.MonotoneX} value={XYViewGeom.MonotoneX}>
            Smooth
          </Dropdown.Item>
          <Dropdown.Item id={XYViewGeom.Step} value={XYViewGeom.Step}>
            Step
          </Dropdown.Item>
        </Dropdown>
      </Form.Element>
    </Grid.Column>
  )
}

export default Geom
