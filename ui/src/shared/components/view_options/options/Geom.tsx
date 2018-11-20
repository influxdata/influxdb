// Libraries
import React, {SFC} from 'react'

// Components
import {Dropdown, FormElement} from 'src/clockface'

// Types
import {XYViewGeom} from 'src/types/v2/dashboards'

interface Props {
  geom: XYViewGeom
  onSetGeom: (geom: XYViewGeom) => void
}

const Geom: SFC<Props> = ({geom, onSetGeom}) => {
  return (
    <FormElement label="Geometry">
      <Dropdown selectedID={geom} onChange={onSetGeom}>
        <Dropdown.Item id={XYViewGeom.Line} value={XYViewGeom.Line}>
          Line
        </Dropdown.Item>
        <Dropdown.Item id={XYViewGeom.Stacked} value={XYViewGeom.Stacked}>
          Stacked
        </Dropdown.Item>
        <Dropdown.Item id={XYViewGeom.Step} value={XYViewGeom.Step}>
          Step
        </Dropdown.Item>
        <Dropdown.Item id={XYViewGeom.Bar} value={XYViewGeom.Bar}>
          Bar
        </Dropdown.Item>
      </Dropdown>
    </FormElement>
  )
}

export default Geom
