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
import {FeatureFlag} from 'src/shared/utils/featureFlag'

// Types
import {PipeContextProps} from 'src/notebooks'

// Contexts
import {NotebookContext} from 'src/notebooks/context/notebook.current'
import {RefContext} from 'src/notebooks/context/refs'

export interface Props extends PipeContextProps {
  id: string
}

export interface HeaderProps {
  id: string
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

  const moveUp = useCallback(() => {
    if (canBeMovedUp) {
      notebook.data.move(id, index - 1)
    }
  }, [index, canBeMovedUp, notebook.data])

  const moveDown = useCallback(() => {
    if (canBeMovedDown) {
      notebook.data.move(id, index + 1)
    }
  }, [index, canBeMovedDown, notebook.data])

  const remove = useCallback(() => removePipe(), [removePipe, id])

  return (
    <div className="notebook-panel--header">
      <div className="notebook-panel--node-wrapper">
        <div className="notebook-panel--node" />
      </div>
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
        <FeatureFlag name="notebook-move-cells">
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
        </FeatureFlag>
        <PanelVisibilityToggle id={id} />
        <RemovePanelButton onRemove={remove} />
      </FlexBox>
    </div>
  )
}

const NotebookPanel: FC<Props> = ({id, children, controls}) => {
  const {notebook} = useContext(NotebookContext)
  const refs = useContext(RefContext)
  const panelRef = useRef<HTMLDivElement>(null)

  const isVisible = notebook.meta.get(id).visible
  const isFocused = refs.get(id).focus

  const panelClassName = classnames('notebook-panel', {
    [`notebook-panel__visible`]: isVisible,
    [`notebook-panel__hidden`]: !isVisible,
    'notebook-panel__focus': isFocused,
  })

  useEffect(() => {
    refs.update(id, {panel: panelRef})
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const updatePanelFocus = useCallback(
    (focus: boolean): void => {
      refs.update(id, {focus})
    },
    [id, refs] // eslint-disable-line react-hooks/exhaustive-deps
  )

  const handleClick = (e: MouseEvent<HTMLDivElement>): void => {
    e.stopPropagation()
    updatePanelFocus(true)
  }

  const handleClickOutside = (): void => {
    updatePanelFocus(false)
  }

  if (
    notebook.readOnly &&
    !/^(visualization|markdown)$/.test(notebook.data.get(id).type)
  ) {
    return null
  }
  return (
    <ClickOutside onClickOutside={handleClickOutside}>
      <div className={panelClassName} onClick={handleClick} ref={panelRef}>
        <NotebookPanelHeader id={id} controls={controls} />
        <div className="notebook-panel--body">{children}</div>
        {!notebook.readOnly && <InsertCellButton id={id} />}
      </div>
    </ClickOutside>
  )
}

export default NotebookPanel
