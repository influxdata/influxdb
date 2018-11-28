// Libraries
import React, {PureComponent} from 'react'
import {Controlled as ReactCodeMirror} from 'react-codemirror2'

// Utils
import {humanizeNote} from 'src/dashboards/utils/notes'

// Styles
import 'src/dashboards/components/NoteEditorText.scss'

const OPTIONS = {
  mode: 'markdown',
  theme: 'markdown',
  tabIndex: 1,
  readonly: false,
  lineNumbers: false,
  autoRefresh: true,
  completeSingle: false,
  lineWrapping: true,
}

const noOp = () => {}

interface Props {
  note: string
  onChangeNote: (value: string) => void
}

class NoteEditorText extends PureComponent<Props, {}> {
  public render() {
    const {note} = this.props

    return (
      <div className="note-editor-text">
        <ReactCodeMirror
          autoCursor={true}
          value={humanizeNote(note)}
          options={OPTIONS}
          onBeforeChange={this.handleChange}
          onTouchStart={noOp}
        />
      </div>
    )
  }

  private handleChange = (_, __, note: string) => {
    const {onChangeNote} = this.props

    onChangeNote(note)
  }
}

export default NoteEditorText
