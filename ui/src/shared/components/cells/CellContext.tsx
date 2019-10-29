// Libraries
import React, {FunctionComponent, useRef, RefObject, useState} from 'react'
import {get} from 'lodash'
import classnames from 'classnames'

// Components
import {
  Popover,
  PopoverType,
  Icon,
  IconFont,
  PopoverInteraction,
} from '@influxdata/clockface'

// Types
import {Cell, View} from 'src/types'

interface Props {
  cell: Cell
  view: View
  onDeleteCell: (cell: Cell) => void
  onCloneCell: (cell: Cell) => void
  onCSVDownload: () => void
  onEditCell: () => void
  onEditNote: (id: string) => void
}

const CellContext: FunctionComponent<Props> = ({
  view,
  cell,
  onEditNote,
  onEditCell,
  onDeleteCell,
  onCloneCell,
  onCSVDownload,
}) => {
  const [popoverVisible, setPopoverVisibility] = useState<boolean>(false)
  const editNoteText = !!get(view, 'properties.note') ? 'Edit Note' : 'Add Note'
  const triggerRef: RefObject<HTMLButtonElement> = useRef<HTMLButtonElement>(
    null
  )
  const buttonClass = classnames('cell--context', {
    'cell--context__active': popoverVisible,
  })

  const handleEditNote = (): void => {
    onEditNote(view.id)
  }

  const handleCloneCell = (): void => {
    onCloneCell(cell)
  }

  const handleDeleteCell = (): void => {
    onDeleteCell(cell)
  }

  const popoverContents = (onHide): JSX.Element => {
    if (view.properties.type === 'markdown') {
      return (
        <div className="cell--context-menu" onClick={onHide}>
          <div className="cell--context-item" onClick={handleEditNote}>
            Edit Note
          </div>
        </div>
      )
    }

    return (
      <div className="cell--context-menu" onClick={onHide}>
        <div className="cell--context-item" onClick={onEditCell}>
          Configure
        </div>
        <div className="cell--context-item" onClick={handleCloneCell}>
          Clone
        </div>
        <div className="cell--context-item" onClick={handleDeleteCell}>
          Delete
        </div>
        <div className="cell--context-item" onClick={handleEditNote}>
          {editNoteText}
        </div>
        <div className="cell--context-item" onClick={onCSVDownload}>
          Download CSV
        </div>
      </div>
    )
  }

  return (
    <>
      <button className={buttonClass} ref={triggerRef}>
        <Icon glyph={IconFont.CogThick} />
      </button>
      <Popover
        type={PopoverType.Solid}
        enableDefaultStyles={false}
        showEvent={PopoverInteraction.Hover}
        hideEvent={PopoverInteraction.Hover}
        triggerRef={triggerRef}
        contents={popoverContents}
        onShow={() => {
          setPopoverVisibility(true)
        }}
        onHide={() => {
          setPopoverVisibility(false)
        }}
      />
    </>
  )
}

export default CellContext
