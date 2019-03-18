// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Stack,
  Alignment,
  SlideToggle,
  ComponentSize,
  ComponentSpacer,
} from '@influxdata/clockface'
import NoteEditorText from 'src/dashboards/components/NoteEditorText'
import NoteEditorPreview from 'src/dashboards/components/NoteEditorPreview'

// Actions
import {
  setIsPreviewing,
  toggleShowNoteWhenEmpty,
  setNote,
} from 'src/dashboards/actions/notes'

// Styles
import 'src/dashboards/components/NoteEditor.scss'

// Types
import {AppState} from 'src/types/v2'

interface StateProps {
  note: string
  showNoteWhenEmpty: boolean
}

interface DispatchProps {
  onSetIsPreviewing: typeof setIsPreviewing
  onToggleShowNoteWhenEmpty: typeof toggleShowNoteWhenEmpty
  onSetNote: typeof setNote
}

interface OwnProps {}

type Props = StateProps & DispatchProps & OwnProps

class NoteEditor extends PureComponent<Props> {
  public render() {
    const {note, onSetNote} = this.props

    return (
      <div className="note-editor">
        <div className="note-editor--controls">
          <div className="note-editor--helper">
            Need help using Markdown? Check out{' '}
            <a
              href="https://daringfireball.net/projects/markdown/syntax"
              target="_blank"
            >
              this handy guide
            </a>
          </div>
          {this.visibilityToggle}
        </div>
        <div className="note-editor--body">
          <NoteEditorText note={note} onChangeNote={onSetNote} />
          <NoteEditorPreview note={note} />
        </div>
      </div>
    )
  }

  private get visibilityToggle(): JSX.Element {
    const {showNoteWhenEmpty, onToggleShowNoteWhenEmpty} = this.props

    return (
      <ComponentSpacer stackChildren={Stack.Columns} align={Alignment.Right}>
        <SlideToggle.Label text="Show note when query returns no data" />
        <SlideToggle
          active={showNoteWhenEmpty}
          size={ComponentSize.ExtraSmall}
          onChange={onToggleShowNoteWhenEmpty}
        />
      </ComponentSpacer>
    )
  }
}

const mstp = (state: AppState) => {
  const {note, isPreviewing, showNoteWhenEmpty} = state.noteEditor

  return {note, isPreviewing, showNoteWhenEmpty}
}

const mdtp = {
  onSetIsPreviewing: setIsPreviewing,
  onToggleShowNoteWhenEmpty: toggleShowNoteWhenEmpty,
  onSetNote: setNote,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(NoteEditor)
