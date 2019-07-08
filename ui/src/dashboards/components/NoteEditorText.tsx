// Libraries
import React, {PureComponent} from 'react'
import {Controlled as ReactCodeMirror, IInstance} from 'react-codemirror2'
import {ScrollInfo} from 'codemirror'

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
  onScroll: (scrollTop: number) => void
  scrollTop: number
}

class NoteEditorText extends PureComponent<Props, {}> {
  private editor: IInstance

  public componentDidUpdate() {
    const currentScrollTop = this.editor.getScrollInfo().top
    if (this.props.scrollTop !== currentScrollTop) {
      this.editor.scrollTo(0, this.props.scrollTop)
    }
  }

  public render() {
    const {note} = this.props

    return (
      <ReactCodeMirror
        autoCursor={true}
        value={humanizeNote(note)}
        options={OPTIONS}
        onBeforeChange={this.handleChange}
        onTouchStart={noOp}
        editorDidMount={this.handleMount}
        onScroll={this.handleScroll}
      />
    )
  }

  private handleMount = (instance: IInstance) => {
    instance.focus()
    this.editor = instance
  }

  private handleChange = (_, __, note: string) => {
    const {onChangeNote} = this.props

    onChangeNote(note)
  }

  private handleScroll = (__: IInstance, scrollInfo: ScrollInfo) => {
    const {onScroll} = this.props

    onScroll(scrollInfo.top)
  }
}

export default NoteEditorText
