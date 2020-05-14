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

type PanelState = 'hidden' | 'small' | 'large'
interface ProgressMap {
  [key: PanelState]: PanelState
}

const PANEL_PROGRESS: ProgressMap = {
  hidden: 'small',
  small: 'large',
  large: 'hidden',
}

const Panel: FC<Props> = ({
  children,
  title,
  controlsLeft,
  controlsRight,
  onRemove,
}) => {
  const [panelState, setPanelState] = useState<PanelState>('small')

  const panelClassName = classnames('notebook-panel', {
    [`notebook-panel__${panelState}`]: panelState,
  })
  const childrenShouldBeVisible =
    panelState === 'small' || panelState === 'large'

  const handleToggle = (): void => {
    setPanelState(PANEL_PROGRESS[panelState])
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
          {childrenShouldBeVisible && controlsLeft}
        </FlexBox>
        <FlexBox
          className="notebook-panel--header-right"
          alignItems={AlignItems.Center}
          margin={ComponentSize.Small}
          justifyContent={JustifyContent.FlexEnd}
        >
          {childrenShouldBeVisible && controlsRight}
          {removePanelButton}
        </FlexBox>
      </div>
      <div className="notebook-panel--body">
        {childrenShouldBeVisible && children}
      </div>
    </div>
  )
}

export {Panel}

export default Panel
