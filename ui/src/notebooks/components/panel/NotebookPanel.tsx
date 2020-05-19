// Libraries
import React, {FC, useContext} from 'react'

// Components
import {
  FlexBox,
  ComponentSize,
  AlignItems,
  JustifyContent,
} from '@influxdata/clockface'
import {NotebookContext} from 'src/notebooks/context/notebook'
import RemovePanelButton from 'src/notebooks/components/panel/RemovePanelButton'
import PanelVisibilityToggle from 'src/notebooks/components/panel/PanelVisibilityToggle'
import MovePanelButton from 'src/notebooks/components/panel/MovePanelButton'
import NotebookPanelTitle from 'src/notebooks/components/panel/NotebookPanelTitle'

export interface Props {
  index: number
}

const NotebookPanel: FC<Props> = ({index}) => {
  const {pipes, removePipe, movePipe} = useContext(NotebookContext)
  const canBeMovedUp = index > 0
  const canBeMovedDown = index < pipes.length - 1
  const canBeRemoved = index !== 0

  const moveUp = canBeMovedUp ? () => movePipe(index, index - 1) : null
  const moveDown = canBeMovedDown ? () => movePipe(index, index + 1) : null
  const remove = canBeRemoved ? () => removePipe(index) : null

  return (
    <div className="notebook-panel--header">
      <FlexBox
        className="notebook-panel--header-left"
        alignItems={AlignItems.Center}
        margin={ComponentSize.Small}
        justifyContent={JustifyContent.FlexStart}
      >
        <NotebookPanelTitle index={index} />
      </FlexBox>
      <FlexBox
        className="notebook-panel--header-right"
        alignItems={AlignItems.Center}
        margin={ComponentSize.Small}
        justifyContent={JustifyContent.FlexEnd}
      >
        <MovePanelButton direction="up" onClick={moveUp} />
        <MovePanelButton direction="down" onClick={moveDown} />
        <PanelVisibilityToggle index={index} />
        <RemovePanelButton onRemove={remove} />
      </FlexBox>
    </div>
  )
}

export default NotebookPanel
