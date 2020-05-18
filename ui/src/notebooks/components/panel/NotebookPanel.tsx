// Libraries
import React, {FC, ReactChildren, useState} from 'react'
import classnames from 'classnames'

// Components
import {
  FlexBox,
  ComponentSize,
  ComponentStatus,
  AlignItems,
  JustifyContent,
  SquareButton,
  IconFont,
} from '@influxdata/clockface'
import RemovePanelButton from 'src/notebooks/components/panel/RemovePanelButton'
import NotebookPanelTitle from 'src/notebooks/components/panel/NotebookPanelTitle'

interface Props {
  id: string
  children: ReactChildren | JSX.Element | JSX.Element[]
  title: string
  onTitleChange?: (title: string) => void
  previousPanelTitle?: string
  controlsLeft?: JSX.Element | JSX.Element[]
  controlsRight?: JSX.Element | JSX.Element[]
  onRemove?: (id?: string) => void
  onMoveUp?: (id?: string) => void
  onMoveDown?: (id?: string) => void
}

const NotebookPanel: FC<Props> = ({
  id,
  children,
  title,
  previousPanelTitle,
  onTitleChange,
  controlsLeft,
  controlsRight,
  onRemove,
  onMoveUp,
  onMoveDown,
}) => {
  const [panelState, setPanelState] = useState<'hidden' | 'visible'>('visible')

  const panelClassName = classnames('notebook-panel', {
    [`notebook-panel__${panelState}`]: panelState,
  })

  const toggleIcon =
    panelState === 'visible' ? IconFont.EyeOpen : IconFont.EyeClosed

  const handleToggle = (): void => {
    if (panelState === 'hidden') {
      setPanelState('visible')
    } else if (panelState === 'visible') {
      setPanelState('hidden')
    }
  }

  const movePanelUpButtonStatus = onMoveUp
    ? ComponentStatus.Default
    : ComponentStatus.Disabled

  const movePanelDownButtonStatus = onMoveDown
    ? ComponentStatus.Default
    : ComponentStatus.Disabled

  const handleMovePanelUp = (): void => {
    if (onMoveUp) {
      onMoveUp(id)
    }
  }

  const handleMovePanelDown = (): void => {
    if (onMoveDown) {
      onMoveDown(id)
    }
  }

  return (
    <div className={panelClassName}>
      <div className="notebook-panel--header">
        <FlexBox
          className="notebook-panel--header-left"
          alignItems={AlignItems.Center}
          margin={ComponentSize.Small}
          justifyContent={JustifyContent.FlexStart}
        >
          <NotebookPanelTitle
            title={title}
            onTitleChange={onTitleChange}
            previousPanelTitle={previousPanelTitle}
          />
          {controlsLeft}
        </FlexBox>
        <FlexBox
          className="notebook-panel--header-right"
          alignItems={AlignItems.Center}
          margin={ComponentSize.Small}
          justifyContent={JustifyContent.FlexEnd}
        >
          {controlsRight}
          <SquareButton
            icon={IconFont.CaretUp}
            status={movePanelUpButtonStatus}
            onClick={handleMovePanelUp}
            titleText="Move this panel up"
          />
          <SquareButton
            icon={IconFont.CaretDown}
            status={movePanelDownButtonStatus}
            onClick={handleMovePanelDown}
            titleText="Move this panel down"
          />
          <SquareButton icon={toggleIcon} onClick={handleToggle} />
          <RemovePanelButton id={id} onRemove={onRemove} />
        </FlexBox>
      </div>
      <div className="notebook-panel--body">
        {panelState === 'visible' && children}
      </div>
    </div>
  )
}

export default NotebookPanel
