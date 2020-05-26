import React, {FC} from 'react'

import {Page} from '@influxdata/clockface'
import AddButtons from 'src/notebooks/components/AddButtons'
import Buttons from 'src/notebooks/components/header/Buttons'

const FULL_WIDTH = true

const Header: FC = () => (
  <>
    <Page.Header fullWidth={FULL_WIDTH}>
      <Page.Title title="Notebooks" />
    </Page.Header>
    <Page.ControlBar fullWidth={FULL_WIDTH}>
      <Page.ControlBarLeft>
        <AddButtons />
      </Page.ControlBarLeft>
      <Page.ControlBarRight>
        <Buttons />
      </Page.ControlBarRight>
    </Page.ControlBar>
  </>
)

export default Header
