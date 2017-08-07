import React, {PropTypes, Component} from 'react'
import CodeMirror from '@skidding/react-codemirror'
import 'src/external/codemirror'

class TickscriptEditor extends Component {
  constructor(props) {
    super(props)
  }

  updateCode(script) {
    this.props.onChangeScript(script)
  }

  render() {
    const {script} = this.props

    const options = {
      lineNumbers: true,
      theme: 'material',
    }

    return (
      <CodeMirror
        value={script}
        onChange={::this.updateCode}
        options={options}
      />
    )
  }
}

const {func, string} = PropTypes

TickscriptEditor.propTypes = {
  onChangeScript: func,
  script: string,
}

export default TickscriptEditor
