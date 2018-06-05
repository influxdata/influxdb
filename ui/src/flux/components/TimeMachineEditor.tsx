import React, {PureComponent} from 'react'
import {Controlled as ReactCodeMirror, IInstance} from 'react-codemirror2'
import {EditorChange} from 'codemirror'
import {ShowHintOptions} from 'src/types/codemirror'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {OnChangeScript, OnSubmitScript, Suggestion} from 'src/types/flux'
import {getFluxCompletions} from 'src/flux/helpers/autoComplete'

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
  suggestions: Suggestion[]
}

interface EditorInstance extends IInstance {
  showHint: (options?: ShowHintOptions) => void
}

@ErrorHandling
class TimeMachineEditor extends PureComponent<Props> {
  private editor: EditorInstance

  constructor(props) {
    super(props)
  }

  public componentDidUpdate(prevProps) {
    const {status, visibility} = this.props

    if (status.type === 'error') {
      this.makeError()
    }

    if (status.type !== 'error') {
      this.editor.clearGutter('error-gutter')
    }

    if (prevProps.visibility === visibility) {
      return
    }

    if (visibility === 'visible') {
      setTimeout(() => this.editor.refresh(), 60)
    }
  }

  public render() {
    const {script} = this.props

    const options = {
      lineNumbers: true,
      theme: 'time-machine',
      tabIndex: 1,
      readonly: false,
      completeSingle: false,
      autoRefresh: true,
      mode: 'flux',
      gutters: ['error-gutter'],
    }

    return (
      <div className="time-machine-editor">
        <ReactCodeMirror
          autoFocus={true}
          autoCursor={true}
          value={script}
          options={options}
          onBeforeChange={this.updateCode}
          onTouchStart={this.onTouchStart}
          editorDidMount={this.handleMount}
          onBlur={this.handleBlur}
          onKeyUp={this.handleKeyUp}
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

  private onTouchStart = () => {}

  private handleKeyUp = (editor: EditorInstance, e: KeyboardEvent) => {
    const {suggestions} = this.props
    const space = ' '

    if (e.ctrlKey && e.key === space) {
      editor.showHint({
        hint: () => getFluxCompletions(this.editor, suggestions),
      })
    }
  }

  private updateCode = (
    _: IInstance,
    __: EditorChange,
    script: string
  ): void => {
    this.props.onChangeScript(script)
  }
}

export default TimeMachineEditor
