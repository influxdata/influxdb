// Libraries
import React, {FC, ReactChildren, useContext} from 'react'
import classnames from 'classnames'

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
  children: ReactChildren | JSX.Element | JSX.Element[]
  onMoveUp?: () => void
  onMoveDown?: () => void
  onRemove?: () => void
}

const NotebookPanel: FC<Props> = ({
  index,
  children,
  onMoveUp,
  onMoveDown,
  onRemove,
}) => {
  const {meta} = useContext(NotebookContext)
  const isVisible = meta[index].visible

  const panelClassName = classnames('notebook-panel', {
    [`notebook-panel__visible`]: isVisible,
    [`notebook-panel__hidden`]: !isVisible,
  })

  return (
    <div className={panelClassName}>
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
          <MovePanelButton direction="up" onClick={onMoveUp} />
          <MovePanelButton direction="down" onClick={onMoveDown} />
          <PanelVisibilityToggle index={index} />
          <RemovePanelButton onRemove={onRemove} />
        </FlexBox>
      </div>
      <div className="notebook-panel--body">{isVisible && children}</div>
    </div>
  )
}

export default NotebookPanel
