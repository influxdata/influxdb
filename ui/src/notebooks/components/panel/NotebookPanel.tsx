// Libraries
import React, {
  FC,
  useContext,
  useCallback,
  useEffect,
  ReactNode,
  MouseEvent,
  useRef,
} from 'react'
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
import {PipeContextProps} from 'src/notebooks'

// Contexts
import {NotebookContext, PipeMeta} from 'src/notebooks/context/notebook'

export interface Props extends PipeContextProps {
  index: number
}

export interface HeaderProps {
  index: number
  controls?: ReactNode
}

const NotebookPanelHeader: FC<HeaderProps> = ({index, controls}) => {
  const {pipes, removePipe, movePipe} = useContext(NotebookContext)
  const canBeMovedUp = index > 0
  const canBeMovedDown = index < pipes.length - 1

  const moveUp = useCallback(() => {
    if (canBeMovedUp) {
      movePipe(index, index - 1)
    }
  }, [index, canBeMovedUp, movePipe])

  const moveDown = useCallback(() => {
    if (canBeMovedDown) {
      movePipe(index, index + 1)
    }
  }, [index, canBeMovedDown, movePipe])

  const remove = useCallback(() => removePipe(index), [removePipe, index])

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
        {controls}
        <MovePanelButton
          direction="up"
          onClick={moveUp}
          active={canBeMovedUp}
        />
        <MovePanelButton
          direction="down"
          onClick={moveDown}
          active={canBeMovedDown}
        />
        <PanelVisibilityToggle index={index} />
        <RemovePanelButton onRemove={remove} />
      </FlexBox>
    </div>
  )
}

const NotebookPanel: FC<Props> = ({index, children, controls}) => {
  const {meta, updateMeta} = useContext(NotebookContext)
  const panelRef = useRef<HTMLDivElement>(null)

  const isVisible = meta[index].visible
  const isFocused = meta[index].focus

  useEffect(() => {
    updateMeta(index, {
      panelRef,
    } as PipeMeta)
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const panelClassName = classnames('notebook-panel', {
    [`notebook-panel__visible`]: isVisible,
    [`notebook-panel__hidden`]: !isVisible,
    'notebook-panel__focus': isFocused,
  })

  const updatePanelFocus = useCallback(
    (focus: boolean): void => {
      updateMeta(index, {
        focus,
      } as PipeMeta)
    },
    [index, meta] // eslint-disable-line react-hooks/exhaustive-deps
  )

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
          <NotebookPanelHeader index={index} controls={controls} />
          <div className="notebook-panel--body">{children}</div>
        </div>
      </ClickOutside>
      <InsertCellButton index={index} />
    </>
  )
}

export default NotebookPanel
