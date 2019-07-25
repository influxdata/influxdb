// Libraries
import React, {SFC} from 'react'

// Components
import {Dropdown, Form, Grid} from '@influxdata/clockface'

// Utils
import {resolveGeom} from 'src/shared/utils/vis'

// Types
import {XYViewGeom} from 'src/types'
import {Columns} from '@influxdata/clockface'

interface Props {
  geom: XYViewGeom
  onSetGeom: (geom: XYViewGeom) => void
}

const getGeomLabel = (geom: XYViewGeom): string => {
  switch (geom) {
    case XYViewGeom.MonotoneX:
      return 'Smooth'
    case XYViewGeom.Step:
      return 'Step'
    default:
    case XYViewGeom.Line:
      return 'Linear'
  }
}

const Geom: SFC<Props> = ({geom, onSetGeom}) => {
  return (
    <Grid.Column widthXS={Columns.Twelve}>
      <Form.Element label="Interpolation">
        <Dropdown
          button={(active, onClick) => (
            <Dropdown.Button active={active} onClick={onClick}>
              {getGeomLabel(resolveGeom(geom))}
            </Dropdown.Button>
          )}
          menu={onCollapse => (
            <Dropdown.Menu onCollapse={onCollapse}>
              <Dropdown.Item
                value={XYViewGeom.Line}
                onClick={onSetGeom}
                selected={geom === XYViewGeom.Line}
              >
                Linear
              </Dropdown.Item>
              <Dropdown.Item
                value={XYViewGeom.MonotoneX}
                onClick={onSetGeom}
                selected={geom === XYViewGeom.MonotoneX}
              >
                Smooth
              </Dropdown.Item>
              <Dropdown.Item
                value={XYViewGeom.Step}
                onClick={onSetGeom}
                selected={geom === XYViewGeom.Step}
              >
                Step
              </Dropdown.Item>
            </Dropdown.Menu>
          )}
        />
      </Form.Element>
    </Grid.Column>
  )
}

export default Geom
