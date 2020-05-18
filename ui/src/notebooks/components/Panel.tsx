// Libraries
import React, {FC, ReactChildren} from 'react'

// Components
import {
  FlexBox,
  ComponentSize,
  AlignItems,
  JustifyContent,
} from '@influxdata/clockface'

import usePanelState from 'src/notebooks/hooks/usePanelState'
import RemoveButton from 'src/notebooks/components/RemoveButton'

interface Props {
  children: ReactChildren | JSX.Element | JSX.Element[]
  title: string
  controlsLeft?: JSX.Element | JSX.Element[]
  controlsRight?: JSX.Element | JSX.Element[]
  onRemove?: () => void
}

const Panel: FC<Props> = ({
  children,
  title,
  controlsLeft,
  controlsRight,
  onRemove,
}) => {
  const [className, showChildren, toggle] = usePanelState()

  return (
    <div className={className}>
      <div className="notebook-panel--header">
        <FlexBox
          className="notebook-panel--header-left"
          alignItems={AlignItems.Center}
          margin={ComponentSize.Small}
          justifyContent={JustifyContent.FlexStart}
        >
          <div className="notebook-panel--toggle" onClick={toggle} />
          <div className="notebook-panel--title">{title}</div>
          {showChildren && controlsLeft}
        </FlexBox>
        <FlexBox
          className="notebook-panel--header-right"
          alignItems={AlignItems.Center}
          margin={ComponentSize.Small}
          justifyContent={JustifyContent.FlexEnd}
        >
          {showChildren && controlsRight}
          <RemoveButton onRemove={onRemove} />
        </FlexBox>
      </div>
      <div className="notebook-panel--body">{showChildren && children}</div>
    </div>
  )
}

export {Panel}

export default Panel
