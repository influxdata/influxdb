import React, {FC, useContext} from 'react'

import {Page} from '@influxdata/clockface'
import AddButtons from 'src/notebooks/components/AddButtons'
import Buttons from 'src/notebooks/components/header/Buttons'
import {NotebookContext} from 'src/notebooks/context/notebook.current'

const FULL_WIDTH = true

const Header: FC = () => {
  const {notebook} = useContext(NotebookContext)

  return (
    <>
      <Page.Header fullWidth={FULL_WIDTH}>
        <Page.Title title="Flows" />
      </Page.Header>
      <Page.ControlBar fullWidth={FULL_WIDTH}>
        {!notebook.readOnly && (
          <Page.ControlBarLeft>
            <h3 className="notebook--add-cell-label">Add Cell:</h3>
            <AddButtons eventName="Notebook Add Button Clicked" />
          </Page.ControlBarLeft>
        )}
        <Page.ControlBarRight>
          <Buttons />
        </Page.ControlBarRight>
      </Page.ControlBar>
    </>
  )
}

export default Header
