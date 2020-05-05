// Libraries
import React, {FC, ReactChildren, useState} from 'react'
import classnames from 'classnames'

// Components
import {
  FlexBox,
  ComponentSize,
  AlignItems,
  JustifyContent,
  SquareButton,
  IconFont,
} from '@influxdata/clockface'

interface Props {
  children: ReactChildren | JSX.Element | JSX.Element[]
  title: string
  controlsLeft?: JSX.Element | JSX.Element[]
  controlsRight?: JSX.Element | JSX.Element[]
  onRemove?: () => void
}

const NotebookPanel: FC<Props> = ({
  children,
  title,
  controlsLeft,
  controlsRight,
  onRemove,
}) => {
  const [panelState, setPanelState] = useState<'hidden' | 'small' | 'large'>(
    'small'
  )

  const panelClassName = classnames('notebook-panel', {
    [`notebook-panel__${panelState}`]: panelState,
  })
  const childrenShouldBeVisible =
    panelState === 'small' || panelState === 'large'

  const handleToggle = (): void => {
    if (panelState === 'hidden') {
      setPanelState('small')
    } else if (panelState === 'small') {
      setPanelState('large')
    } else if (panelState === 'large') {
      setPanelState('hidden')
    }
  }

  let removePanelButton

  if (onRemove) {
    removePanelButton = (
      <SquareButton icon={IconFont.Remove} onClick={onRemove} />
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
          <div className="notebook-panel--toggle" onClick={handleToggle} />
          <div className="notebook-panel--title">{title}</div>
          {controlsLeft}
        </FlexBox>
        <FlexBox
          className="notebook-panel--header-right"
          alignItems={AlignItems.Center}
          margin={ComponentSize.Small}
          justifyContent={JustifyContent.FlexEnd}
        >
          {controlsRight}
          {removePanelButton}
        </FlexBox>
      </div>
      <div className="notebook-panel--body">
        {childrenShouldBeVisible && children}
      </div>
    </div>
  )
}

export default NotebookPanel
