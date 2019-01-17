// Libraries
import React, {PureComponent} from 'react'
import {Controlled as ReactCodeMirror} from 'react-codemirror2'

// Utils
import {humanizeNote} from 'src/dashboards/utils/notes'

const OPTIONS = {
  mode: 'markdown',
  theme: 'markdown',
  tabIndex: 1,
  readonly: false,
  lineNumbers: false,
  autoRefresh: true,
  completeSingle: false,
  lineWrapping: true,
  placeholder: 'You can use Markdown syntax to format your note',
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
      <ReactCodeMirror
        autoCursor={true}
        value={humanizeNote(note)}
        options={OPTIONS}
        onBeforeChange={this.handleChange}
        onTouchStart={noOp}
      />
    )
  }

  private handleChange = (_, __, note: string) => {
    const {onChangeNote} = this.props

    onChangeNote(note)
  }
}

export default NoteEditorText
