import React, {PureComponent} from 'react'
import {Controlled as CodeMirror, IInstance} from 'react-codemirror2'
import {EditorChange} from 'codemirror'
import 'src/external/codemirror'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {OnChangeScript, OnSubmitScript} from 'src/types/ifql'
import {editor} from 'src/ifql/constants'

interface Gutter {
  line: number
  text: string
}

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
    this.editor.clearGutter('error-gutter')
    const lineNumbers = this.statusLine
    lineNumbers.forEach(({line, text}) => {
      this.editor.setGutterMarker(
        line - 1,
        'error-gutter',
        this.errorMarker(text)
      )
    })

    this.editor.refresh()
  }

  private errorMarker(message: string): HTMLElement {
    const span = document.createElement('span')
    span.className = 'icon stop error-warning'
    span.title = message
    return span
  }

  private get statusLine(): Gutter[] {
    const {status} = this.props
    const messages = status.text.split('\n')
    const lineNumbers = messages.map(text => {
      const [numbers] = text.split(' ')
      const [lineNumber] = numbers.split(':')
      return {line: Number(lineNumber), text}
    })

    return lineNumbers
  }

  private handleMount = (instance: EditorInstance) => {
    instance.refresh() // required to for proper line height on mount
    this.editor = instance
  }

  private handleKeyUp = (instance: EditorInstance, e: KeyboardEvent) => {
    const {key} = e
    const prevKey = this.prevKey

    if (
      prevKey === 'Control' ||
      prevKey === 'Meta' ||
      (prevKey === 'Shift' && key === '.')
    ) {
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
