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
}

interface EditorInstance extends IInstance {
  showHint: (options?: any) => void
}

@ErrorHandling
class TimeMachineEditor extends PureComponent<Props> {
  constructor(props) {
    super(props)
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
    }

    return (
      <div className="time-machine-editor">
        <CodeMirror
          autoFocus={true}
          autoCursor={true}
          value={script}
          options={options}
          onBeforeChange={this.updateCode}
          onTouchStart={this.onTouchStart}
          onKeyUp={this.handleKeyUp}
        />
      </div>
    )
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
