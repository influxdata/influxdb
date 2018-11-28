// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'

// Components
import {Radio, SlideToggle, ComponentSize} from 'src/clockface'
import NoteEditorText from 'src/dashboards/components/NoteEditorText'
import NoteEditorPreview from 'src/dashboards/components/NoteEditorPreview'

// Actions
import {
  setIsPreviewing,
  toggleShowNoteWhenEmpty,
  setNote,
} from 'src/dashboards/actions/v2/notes'

// Styles
import 'src/dashboards/components/NoteEditor.scss'

// Types
import {AppState} from 'src/types/v2'

interface StateProps {
  note: string
  isPreviewing: boolean
  toggleVisible: boolean
  showNoteWhenEmpty: boolean
}

interface DispatchProps {
  onSetIsPreviewing: typeof setIsPreviewing
  onToggleShowNoteWhenEmpty: typeof toggleShowNoteWhenEmpty
  onSetNote: typeof setNote
}

interface OwnProps {}

type Props = StateProps & DispatchProps & OwnProps

const NoteEditor: SFC<Props> = props => {
  const {
    note,
    isPreviewing,
    toggleVisible,
    showNoteWhenEmpty,
    onSetIsPreviewing,
    onToggleShowNoteWhenEmpty,
    onSetNote,
  } = props

  return (
    <div className="note-editor">
      <div
        className={`note-editor--controls ${toggleVisible ? '' : 'centered'}`}
      >
        <Radio>
          <Radio.Button
            value={false}
            active={!isPreviewing}
            onClick={onSetIsPreviewing}
          >
            Compose
          </Radio.Button>
          <Radio.Button
            value={true}
            active={isPreviewing}
            onClick={onSetIsPreviewing}
          >
            Preview
          </Radio.Button>
        </Radio>
        {toggleVisible && (
          <label className="note-editor--toggle">
            Show note when query returns no data
            <SlideToggle
              active={showNoteWhenEmpty}
              size={ComponentSize.ExtraSmall}
              onChange={onToggleShowNoteWhenEmpty}
            />
          </label>
        )}
      </div>
      {isPreviewing ? (
        <NoteEditorPreview note={note} />
      ) : (
        <NoteEditorText note={note} onChangeNote={onSetNote} />
      )}
    </div>
  )
}

const mstp = (state: AppState) => {
  const {
    note,
    isPreviewing,
    toggleVisible,
    showNoteWhenEmpty,
  } = state.noteEditor

  return {note, isPreviewing, toggleVisible, showNoteWhenEmpty}
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
