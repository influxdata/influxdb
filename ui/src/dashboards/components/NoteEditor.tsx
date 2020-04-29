// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  SlideToggle,
  InputLabel,
  ComponentSize,
  FlexBox,
  FlexDirection,
  JustifyContent,
} from '@influxdata/clockface'
import NoteEditorText from 'src/dashboards/components/NoteEditorText'
import NoteEditorPreview from 'src/dashboards/components/NoteEditorPreview'

// Actions
import {
  setIsPreviewing,
  toggleShowNoteWhenEmpty,
  setNote,
} from 'src/dashboards/actions/notes'

// Types
import {AppState, NoteEditorMode} from 'src/types'

interface StateProps {
  note: string
  showNoteWhenEmpty: boolean
  hasQuery: boolean
}

interface DispatchProps {
  onSetIsPreviewing: typeof setIsPreviewing
  onToggleShowNoteWhenEmpty: typeof toggleShowNoteWhenEmpty
  onSetNote: typeof setNote
}

interface OwnProps {}

type Props = StateProps & DispatchProps & OwnProps

interface State {
  scrollTop: number
}

class NoteEditor extends PureComponent<Props, State> {
  public state = {scrollTop: 0}

  public render() {
    const {note, onSetNote} = this.props
    const {scrollTop} = this.state

    return (
      <div className="note-editor">
        <div className="note-editor--controls">
          <div className="note-editor--helper">
            Need help using Markdown? Check out{' '}
            <a href="https://www.markdownguide.org/cheat-sheet" target="_blank">
              this handy guide
            </a>
          </div>
          {this.visibilityToggle}
        </div>
        <div className="note-editor--body">
          <NoteEditorText
            note={note}
            onChangeNote={onSetNote}
            onScroll={this.handleEditorScroll}
            scrollTop={scrollTop}
          />
          <NoteEditorPreview
            note={note}
            scrollTop={scrollTop}
            onScroll={this.handlePreviewScroll}
          />
        </div>
      </div>
    )
  }

  private get visibilityToggle(): JSX.Element {
    const {hasQuery, showNoteWhenEmpty, onToggleShowNoteWhenEmpty} = this.props

    if (!hasQuery) {
      return null
    }

    return (
      <FlexBox
        direction={FlexDirection.Row}
        justifyContent={JustifyContent.FlexEnd}
      >
        <InputLabel>Show note when query returns no data</InputLabel>
        <SlideToggle
          active={showNoteWhenEmpty}
          size={ComponentSize.ExtraSmall}
          onChange={onToggleShowNoteWhenEmpty}
        />
      </FlexBox>
    )
  }

  private handleEditorScroll = (scrollTop: number) => {
    this.setState({scrollTop})
  }

  private handlePreviewScroll = (e: MouseEvent<HTMLElement>) => {
    const target = e.target as HTMLElement
    const {scrollTop} = target

    this.setState({scrollTop})
  }
}

const mstp = (state: AppState) => {
  const {note, mode, viewID, isPreviewing, showNoteWhenEmpty} = state.noteEditor
  let hasQuery =
    mode === NoteEditorMode.Editing &&
    viewID &&
    state.resources.views.byID[viewID] &&
    state.resources.views.byID[viewID].properties.type !== 'markdown'

  return {note, hasQuery, isPreviewing, showNoteWhenEmpty}
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
