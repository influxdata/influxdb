// Libraries
import React, {FC, useContext, useMemo, useCallback, ReactNode} from 'react'
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
import RawFluxDataGrid from 'src/timeMachine/components/RawFluxDataGrid'

// Types
import {PipeContextProps} from 'src/notebooks'

// Contexts
import {NotebookContext} from 'src/notebooks/context/notebook'

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
        {controls}
        <MovePanelButton direction="up" onClick={moveUp} />
        <MovePanelButton direction="down" onClick={moveDown} />
        <PanelVisibilityToggle index={index} />
        <RemovePanelButton onRemove={remove} />
      </FlexBox>
    </div>
  )
}

const NotebookResults: FC = ({index}) => {
  const {pipes, results} = useContext(NotebookContext)
  const result = results[index]

  return useMemo(() => {
      if (!result || pipes[index].type !== 'query') {
          return false
      }
      return (
          <div style={{width: '100%',
    overflow: 'auto',
    position: 'relative',
    height: '250px'}}>
      <RawFluxDataGrid
        scrollTop={0}
        scrollLeft={0}
          width={1200}
          height={300}
          data={result.data}
          maxColumnCount={result.maxColumnCount}/>
          </div>
      )
  }, [result])
}

const NotebookPanel: FC<Props> = ({index, children, controls}) => {
  const {meta} = useContext(NotebookContext)

  const isVisible = meta[index].visible

  const panelClassName = classnames('notebook-panel', {
    [`notebook-panel__visible`]: isVisible,
    [`notebook-panel__hidden`]: !isVisible,
  })

  return (
    <div className={panelClassName}>
      <NotebookPanelHeader index={index} controls={controls} />
      <div className="notebook-panel--body">{children}</div>
        <NotebookResults index={index} />
    </div>
  )
}

export default NotebookPanel
