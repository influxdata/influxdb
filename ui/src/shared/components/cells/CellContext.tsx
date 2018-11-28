// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import {Context, IconFont, ComponentColor} from 'src/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {openNoteEditor} from 'src/dashboards/actions/v2/notes'

// Types
import {Cell, View, ViewType} from 'src/types/v2'
import {NoteEditorMode} from 'src/types/v2/dashboards'

interface OwnProps {
  cell: Cell
  view: View
  onDeleteCell: (cell: Cell) => void
  onCloneCell: (cell: Cell) => void
  onCSVDownload: () => void
  onEditCell: () => void
}

interface DispatchProps {
  onOpenNoteEditor: typeof openNoteEditor
}

type Props = DispatchProps & OwnProps

@ErrorHandling
class CellContext extends PureComponent<Props> {
  public render() {
    return (
      <Context className="cell--context">
        <Context.Menu icon={IconFont.Pencil}>{this.editMenuItems}</Context.Menu>
        <Context.Menu
          icon={IconFont.Duplicate}
          color={ComponentColor.Secondary}
        >
          <Context.Item label="Clone" action={this.handleCloneCell} />
        </Context.Menu>
        <Context.Menu icon={IconFont.Trash} color={ComponentColor.Danger}>
          <Context.Item label="Delete" action={this.handleDeleteCell} />
        </Context.Menu>
      </Context>
    )
  }

  private get editMenuItems(): JSX.Element[] | JSX.Element {
    const {view, onEditCell, onCSVDownload} = this.props

    if (view.properties.type === ViewType.Markdown) {
      return <Context.Item label="Edit Note" action={this.handleEditNote} />
    }

    const hasNote = !!get(view, 'properties.note')

    return [
      <Context.Item key="configure" label="Configure" action={onEditCell} />,
      <Context.Item
        key="note"
        label={hasNote ? 'Edit Note' : 'Add Note'}
        action={this.handleEditNote}
      />,
      <Context.Item
        key="download"
        label="Download CSV"
        action={onCSVDownload}
        disabled={true}
      />,
    ]
  }

  private handleDeleteCell = () => {
    const {cell, onDeleteCell} = this.props

    onDeleteCell(cell)
  }

  private handleCloneCell = () => {
    const {cell, onCloneCell} = this.props

    onCloneCell(cell)
  }

  private handleEditNote = () => {
    const {onOpenNoteEditor, view} = this.props

    const note: string = get(view, 'properties.note', '')
    const showNoteWhenEmpty: boolean = get(
      view,
      'properties.showNoteWhenEmpty',
      false
    )

    const initialState = {
      viewID: view.id,
      toggleVisible: view.properties.type !== ViewType.Markdown,
      note,
      showNoteWhenEmpty,
      mode: note === '' ? NoteEditorMode.Adding : NoteEditorMode.Editing,
    }

    onOpenNoteEditor(initialState)
  }
}

const mdtp = {
  onOpenNoteEditor: openNoteEditor,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(CellContext)
