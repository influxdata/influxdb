// Libraries
import React, {FC, ReactChildren, useState, ChangeEvent} from 'react'
import classnames from 'classnames'

// Components
import {
  FlexBox,
  ComponentSize,
  ComponentStatus,
  AlignItems,
  JustifyContent,
  SquareButton,
  Icon,
  IconFont,
} from '@influxdata/clockface'

interface Props {
  id: string
  children: ReactChildren | JSX.Element | JSX.Element[]
  title: string
  onTitleChange?: (title: string) => void
  dataSourceName?: string
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
  dataSourceName,
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

  let removeButton
  let sourceName
  let titleElement = <div className="notebook-panel--title">{title}</div>

  if (onRemove) {
    const handleRemovePanel = (): void => {
      onRemove(id)
    }

    removeButton = (
      <SquareButton icon={IconFont.Remove} onClick={handleRemovePanel} />
    )
  }

  if (dataSourceName) {
    sourceName = (
      <div className="notebook-panel--data-source">
        {dataSourceName}
        <Icon
          glyph={IconFont.CaretRight}
          className="notebook-panel--data-caret"
        />
      </div>
    )
  }

  if (onTitleChange) {
    const onChange = (e: ChangeEvent<HTMLInputElement>): void => {
      const trimmedValue = e.target.value.replace(' ','_')
      onTitleChange(trimmedValue)
    }

    titleElement = (
      <input
        type="text"
        value={title}
        onChange={onChange}
        placeholder="Enter an ID"
        className="notebook-panel--editable-title"
        autoComplete="off"
        autoCorrect="off"
        spellCheck={false}
        maxLength={30}
      />
    )
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
          {sourceName}
          {titleElement}
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
          {removeButton}
        </FlexBox>
      </div>
      <div className="notebook-panel--body">
        {panelState === 'visible' && children}
      </div>
    </div>
  )
}

export default NotebookPanel
