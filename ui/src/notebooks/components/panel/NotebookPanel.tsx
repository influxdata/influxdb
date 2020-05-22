// Libraries
import React, {FC, useContext, useCallback} from 'react'
import classnames from 'classnames'

// Components
import {
  FlexBox,
  ComponentSize,
  AlignItems,
  JustifyContent,
} from '@influxdata/clockface'
import RemovePanelButton from 'src/notebooks/components/panel/RemovePanelButton'
import PanelVisibilityToggle from 'src/notebooks/components/panel/PanelVisibilityToggle'
import MovePanelButton from 'src/notebooks/components/panel/MovePanelButton'
import NotebookPanelTitle from 'src/notebooks/components/panel/NotebookPanelTitle'

// Types
import {PipeContextProps} from 'src/notebooks'

// Contexts
import {NotebookContext} from 'src/notebooks/context/notebook'

export interface Props extends PipeContextProps {
  index: number
}

export interface HeaderProps {
  index: number
}

const NotebookPanelHeader: FC<HeaderProps> = ({index}) => {
  const {pipes, removePipe, movePipe} = useContext(NotebookContext)
  const canBeMovedUp = index > 0
  const canBeMovedDown = index < pipes.length - 1
  const canBeRemoved = index !== 0

  const moveUp = useCallback(
    canBeMovedUp ? () => movePipe(index, index - 1) : null,
    [index, pipes]
  )
  const moveDown = useCallback(
    canBeMovedDown ? () => movePipe(index, index + 1) : null,
    [index, pipes]
  )
  const remove = useCallback(canBeRemoved ? () => removePipe(index) : null, [
    index,
    pipes,
  ])

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

const NotebookPanel: FC<Props> = props => {
  const {index, children} = props
  const {meta} = useContext(NotebookContext)

  const isVisible = meta[index].visible

  const panelClassName = classnames('notebook-panel', {
    [`notebook-panel__visible`]: isVisible,
    [`notebook-panel__hidden`]: !isVisible,
  })

  return (
    <div className={panelClassName}>
      <NotebookPanelHeader index={index} />
      <div className="notebook-panel--body">{children}</div>
    </div>
  )
}

export default NotebookPanel
