import React, {PureComponent} from 'react'
import {Controlled as CodeMirror, IInstance} from 'react-codemirror2'
import {EditorChange} from 'codemirror'
import 'src/external/codemirror'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {OnChangeScript} from 'src/types/ifql'
import {editor} from 'src/ifql/constants'

interface Props {
  script: string
  onChangeScript: OnChangeScript
  visibility: string
}

interface EditorInstance extends IInstance {
  showHint: (options?: any) => void
}

@ErrorHandling
class TimeMachineEditor extends PureComponent<Props> {
  private editor: EditorInstance

  constructor(props) {
    super(props)
  }

  public componentDidUpdate(prevProps) {
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
        />
      </div>
    )
  }

  private handleMount = (instance: EditorInstance) => {
    instance.refresh() // required to for proper line height on mount
    this.editor = instance
  }

  private handleKeyUp = (instance: EditorInstance, e: KeyboardEvent) => {
    const {key} = e

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
