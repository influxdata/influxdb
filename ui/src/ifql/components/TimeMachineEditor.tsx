import React, {PureComponent} from 'react'
import {Controlled as CodeMirror, IInstance} from 'react-codemirror2'
import {EditorChange} from 'codemirror'
import 'src/external/codemirror'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {OnChangeScript, OnSubmitScript} from 'src/types/ifql'
import {editor} from 'src/ifql/constants'

interface Status {
  type: string
  text: string
}

interface Props {
  script: string
  visibility: string
  status: Status
  onChangeScript: OnChangeScript
  onSubmitScript: OnSubmitScript
}

interface EditorInstance extends IInstance {
  showHint: (options?: any) => void
}

@ErrorHandling
class TimeMachineEditor extends PureComponent<Props> {
  private editor: EditorInstance
  private prevKey: string

  constructor(props) {
    super(props)
  }

  public componentDidUpdate(prevProps) {
    if (this.props.status.type === 'error') {
      this.makeError()
    }

    if (this.props.status.type !== 'error') {
      this.editor.clearGutter('error-gutter')
    }

    if (prevProps.visibility === this.props.visibility) {
      return
    }

    if (this.props.visibility === 'visible') {
      setTimeout(() => this.editor.refresh(), 60)
    }
  }

  public render() {
    const {script} = this.props

    const options = {
      lineNumbers: true,
      theme: 'material',
      tabIndex: 1,
      readonly: false,
      extraKeys: {'Ctrl-Space': 'autocomplete'},
      completeSingle: false,
      autoRefresh: true,
      gutters: ['error-gutter'],
    }

    return (
      <div className="time-machine-editor">
        <CodeMirror
          autoFocus={true}
          autoCursor={true}
          value={script}
          options={options}
          onKeyUp={this.handleKeyUp}
          onBeforeChange={this.updateCode}
          onTouchStart={this.onTouchStart}
          editorDidMount={this.handleMount}
          onBlur={this.handleBlur}
        />
      </div>
    )
  }

  private handleBlur = (): void => {
    this.props.onSubmitScript()
  }

  private makeError(): void {
    const {status} = this.props
    this.editor.clearGutter('error-gutter')
    const span = document.createElement('span')
    span.className = 'icon stop error-warning'
    span.title = status.text
    const lineNumber = this.statusLine
    this.editor.setGutterMarker(lineNumber - 1, 'error-gutter', span)
    this.editor.refresh()
  }

  private get statusLine(): number {
    const {status} = this.props
    const numbers = status.text.split(' ')[0]
    const [lineNumber] = numbers.split(':')

    return Number(lineNumber)
  }

  private handleMount = (instance: EditorInstance) => {
    instance.refresh() // required to for proper line height on mount
    this.editor = instance
  }

  private handleKeyUp = (instance: EditorInstance, e: KeyboardEvent) => {
    const {key} = e
    const prevKey = this.prevKey

    if (prevKey === 'Control' || prevKey === 'Meta') {
      return (this.prevKey = key)
    }

    this.prevKey = key

    if (editor.EXCLUDED_KEYS.includes(key)) {
      return
    }

    if (editor.EXCLUDED_KEYS.includes(key)) {
      return
    }

    instance.showHint({completeSingle: false})
  }

  private onTouchStart = () => {}

  private updateCode = (
    _: IInstance,
    __: EditorChange,
    script: string
  ): void => {
    this.props.onChangeScript(script)
  }
}

export default TimeMachineEditor
