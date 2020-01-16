// Libraries
import React, {FC, useRef, RefObject, useState} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'
import classnames from 'classnames'

// Components
import {
  Popover,
  Appearance,
  Icon,
  IconFont,
  PopoverInteraction,
} from '@influxdata/clockface'
import CellContextItem from 'src/shared/components/cells/CellContextItem'
import CellContextDangerItem from 'src/shared/components/cells/CellContextDangerItem'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

// Actions
import {deleteCell} from 'src/cells/actions/thunks'

// Types
import {Cell, View} from 'src/types'

interface DispatchProps {
  onDeleteCell: typeof deleteCell
}

interface OwnProps {
  cell: Cell
  view: View
  onCloneCell: (cell: Cell) => void
  onCSVDownload: () => void
  onEditCell: () => void
  onEditNote: (id: string) => void
}

type Props = OwnProps & DispatchProps

const CellContext: FC<Props> = ({
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
    const {dashboardID, id} = cell

    onDeleteCell(dashboardID, id)
  }

  const popoverContents = (onHide): JSX.Element => {
    if (view.properties.type === 'markdown') {
      return (
        <div className="cell--context-menu">
          <CellContextItem
            label="Edit Note"
            onClick={handleEditNote}
            icon={IconFont.TextBlock}
            onHide={onHide}
            testID="cell-context--note"
          />
          <CellContextDangerItem
            label="Delete"
            onClick={handleDeleteCell}
            icon={IconFont.Trash}
            onHide={onHide}
            testID="cell-context--delete"
          />
        </div>
      )
    }

    return (
      <div className="cell--context-menu">
        <CellContextItem
          label="Configure"
          onClick={onEditCell}
          icon={IconFont.Pencil}
          onHide={onHide}
          testID="cell-context--configure"
        />
        <CellContextItem
          label={editNoteText}
          onClick={handleEditNote}
          icon={IconFont.TextBlock}
          onHide={onHide}
          testID="cell-context--note"
        />
        <CellContextItem
          label="Clone"
          onClick={handleCloneCell}
          icon={IconFont.Duplicate}
          onHide={onHide}
          testID="cell-context--clone"
        />
        <FeatureFlag name="downloadCellCSV">
          <CellContextItem
            label="Download CSV"
            onClick={onCSVDownload}
            icon={IconFont.Download}
            onHide={onHide}
            testID="cell-context--download"
          />
        </FeatureFlag>
        <CellContextDangerItem
          label="Delete"
          onClick={handleDeleteCell}
          icon={IconFont.Trash}
          onHide={onHide}
          testID="cell-context--delete"
        />
      </div>
    )
  }

  return (
    <>
      <button
        className={buttonClass}
        ref={triggerRef}
        data-testid="cell-context--toggle"
      >
        <Icon glyph={IconFont.CogThick} />
      </button>
      <Popover
        appearance={Appearance.Outline}
        enableDefaultStyles={false}
        showEvent={PopoverInteraction.Click}
        hideEvent={PopoverInteraction.Click}
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

const mdtp: DispatchProps = {
  onDeleteCell: deleteCell,
}

export default connect(
  null,
  mdtp
)(CellContext)
