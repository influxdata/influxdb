// Libraries
import React, {FC, ReactChildren, useState} from 'react'
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

export type NotebookPanelVisibility = 'hidden' | 'visible'

const NotebookPanel: FC<Props> = ({
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
  const [panelVisibility, setPanelVisibility] = useState<
    NotebookPanelVisibility
  >('visible')

  const panelClassName = classnames('notebook-panel', {
    [`notebook-panel__${panelVisibility}`]: panelVisibility,
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
          <MovePanelButton direction="up" onClick={onMoveUp} />
          <MovePanelButton direction="down" onClick={onMoveDown} />
          <PanelVisibilityToggle
            onToggle={setPanelVisibility}
            visibility={panelVisibility}
          />
          <RemovePanelButton onRemove={onRemove} />
        </FlexBox>
      </div>
      <div className="notebook-panel--body">
        {panelVisibility === 'visible' && children}
      </div>
    </div>
  )
}

export default NotebookPanel
