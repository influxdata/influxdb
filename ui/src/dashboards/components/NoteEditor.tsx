// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import classnames from 'classnames'

// Components
import {
  Radio,
  SlideToggle,
  ComponentSize,
  ComponentSpacer,
  ButtonShape,
  Stack,
  Alignment,
} from 'src/clockface'
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

class NoteEditor extends PureComponent<Props> {
  public render() {
    const {note, isPreviewing, onSetIsPreviewing, onSetNote} = this.props

    return (
      <div className="note-editor">
        <div className={this.controlsClassName}>
          <div className="note-editor--radio">
            <Radio shape={ButtonShape.StretchToFit}>
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
          </div>
          {this.visibilityToggle}
        </div>
        <div className="note-editor--body">
          {this.noteEditorPreview}
          <NoteEditorText note={note} onChangeNote={onSetNote} />
        </div>
        <div className="note-editor--footer">
          Need help using Markdown? Check out{' '}
          <a
            href="https://daringfireball.net/projects/markdown/syntax"
            target="_blank"
          >
            this handy guide
          </a>
        </div>
      </div>
    )
  }

  private get controlsClassName(): string {
    const {toggleVisible} = this.props

    return classnames('note-editor--controls', {centered: !toggleVisible})
  }

  private get noteEditorPreview(): JSX.Element {
    const {isPreviewing, note} = this.props

    if (isPreviewing) {
      return <NoteEditorPreview note={note} />
    }
  }

  private get visibilityToggle(): JSX.Element {
    const {
      toggleVisible,
      showNoteWhenEmpty,
      onToggleShowNoteWhenEmpty,
    } = this.props

    if (toggleVisible) {
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
