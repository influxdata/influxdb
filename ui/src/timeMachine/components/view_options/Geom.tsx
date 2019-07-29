// Libraries
import React, {SFC} from 'react'

// Components
import {Dropdown, Form, Grid} from '@influxdata/clockface'

// Utils
import {resolveGeom} from 'src/shared/utils/vis'

// Types
import {XYGeom} from 'src/types'
import {Columns} from '@influxdata/clockface'

interface Props {
  geom: XYGeom
  onSetGeom: (geom: XYGeom) => void
}

const getGeomLabel = (geom: XYGeom): string => {
  switch (geom) {
    case 'monotoneX':
      return 'Smooth'
    case 'step':
      return 'Step'
    default:
    case 'line':
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
                value="line"
                onClick={onSetGeom}
                selected={geom === 'line'}
              >
                Linear
              </Dropdown.Item>
              <Dropdown.Item
                value="monotoneX"
                onClick={onSetGeom}
                selected={geom === 'monotoneX'}
              >
                Smooth
              </Dropdown.Item>
              <Dropdown.Item
                value="step"
                onClick={onSetGeom}
                selected={geom === 'step'}
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
