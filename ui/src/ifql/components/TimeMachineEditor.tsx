import React, {PureComponent} from 'react'
import {Controlled as CodeMirror, IInstance} from 'react-codemirror2'
import {EditorChange} from 'codemirror'
import 'src/external/codemirror'

interface Props {
  script: string
  onChangeScript: (script: string) => void
  onSubmitScript: (script: string) => void
}

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
    }

    return (
      <div className="time-machine-editor-container">
        <div className="time-machine-editor">
          <CodeMirror
            value={script}
            options={options}
            onBeforeChange={this.updateCode}
          />
        </div>
        <button className="btn btn-lg btn-primary" onClick={this.handleSubmit}>
          Submit Script
        </button>
      </div>
    )
  }

  private handleSubmit = (): void => {
    this.props.onSubmitScript(this.props.script)
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
