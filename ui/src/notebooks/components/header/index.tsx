import React, {FC} from 'react'

import {Page} from '@influxdata/clockface'
import AddButtons from 'src/notebooks/components/AddButtons'
import Buttons from 'src/notebooks/components/header/Buttons'
import MiniMapToggle from 'src/notebooks/components/minimap/MiniMapToggle'

const FULL_WIDTH = true

const Header: FC = () => (
  <>
    <Page.Header fullWidth={FULL_WIDTH}>
      <Page.Title title="Flows" />
    </Page.Header>
    <Page.ControlBar fullWidth={FULL_WIDTH}>
      <Page.ControlBarLeft>
        <h3 className="notebook--add-cell-label">Add Cell:</h3>
        <AddButtons />
      </Page.ControlBarLeft>
      <Page.ControlBarRight>
        <MiniMapToggle />
        <Buttons />
      </Page.ControlBarRight>
    </Page.ControlBar>
  </>
)

export default Header
