// Libraries
import React, {FC, useContext, ReactNode, MouseEvent, useRef} from 'react'
import classnames from 'classnames'

// Components
import {
  FlexBox,
  ComponentSize,
  AlignItems,
  JustifyContent,
  ClickOutside,
} from '@influxdata/clockface'
import RemovePanelButton from 'src/notebooks/components/panel/RemovePanelButton'
import InsertCellButton from 'src/notebooks/components/panel/InsertCellButton'
import PanelVisibilityToggle from 'src/notebooks/components/panel/PanelVisibilityToggle'
import MovePanelButton from 'src/notebooks/components/panel/MovePanelButton'
import NotebookPanelTitle from 'src/notebooks/components/panel/NotebookPanelTitle'

// Types
import {PipeContextProps, DataID, PipeData} from 'src/notebooks'

// Contexts
import {NotebookContext} from 'src/notebooks/context/notebook.current'

export interface Props extends PipeContextProps {
  id: DataID<PipeData>
}

export interface HeaderProps {
  id: DataID<PipeData>
  controls?: ReactNode
}

const NotebookPanelHeader: FC<HeaderProps> = ({id, controls}) => {
  const {notebook} = useContext(NotebookContext)
  const removePipe = () => {
    notebook.data.remove(id)
    notebook.meta.remove(id)
  }
  const index = notebook.data.indexOf(id)
  const canBeMovedUp = index > 0
  const canBeMovedDown = index < notebook.data.allIDs.length - 1

  const moveUp = canBeMovedUp ? () => notebook.data.move(id, index - 1) : null
  const moveDown = canBeMovedDown
    ? () => notebook.data.move(id, index + 1)
    : null

  return (
    <div className="notebook-panel--header">
      <FlexBox
        className="notebook-panel--header-left"
        alignItems={AlignItems.Center}
        margin={ComponentSize.Small}
        justifyContent={JustifyContent.FlexStart}
      >
        <NotebookPanelTitle id={id} />
      </FlexBox>
      <FlexBox
        className="notebook-panel--header-right"
        alignItems={AlignItems.Center}
        margin={ComponentSize.Small}
        justifyContent={JustifyContent.FlexEnd}
      >
        {controls}
        <MovePanelButton direction="up" onClick={moveUp} />
        <MovePanelButton direction="down" onClick={moveDown} />
        <PanelVisibilityToggle id={id} />
        <RemovePanelButton onRemove={removePipe} />
      </FlexBox>
    </div>
  )
}

const NotebookPanel: FC<Props> = ({id, children, controls}) => {
  const {notebook} = useContext(NotebookContext)
  const panelRef = useRef<HTMLDivElement>(null)

  const isVisible = notebook.meta.get(id).visible
  const isFocused = notebook.meta.get(id).focus

  const panelClassName = classnames('notebook-panel', {
    [`notebook-panel__visible`]: isVisible,
    [`notebook-panel__hidden`]: !isVisible,
    'notebook-panel__focus': isFocused,
  })

  const updatePanelFocus = (focus: boolean): void => {
    if (isFocused === focus) {
      return
    }
    notebook.meta.update(id, {focus})
  }

  const handleClick = (e: MouseEvent<HTMLDivElement>): void => {
    e.stopPropagation()
    updatePanelFocus(true)
  }

  const handleClickOutside = (): void => {
    updatePanelFocus(false)
  }

  return (
    <>
      <ClickOutside onClickOutside={handleClickOutside}>
        <div className={panelClassName} onClick={handleClick} ref={panelRef}>
          <NotebookPanelHeader id={id} controls={controls} />
          <div className="notebook-panel--body">{children}</div>
        </div>
      </ClickOutside>
      <InsertCellButton id={id} />
    </>
  )
}

export default NotebookPanel
