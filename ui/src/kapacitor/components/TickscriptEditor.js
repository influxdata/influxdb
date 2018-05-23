import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {Controlled as CodeMirror} from 'react-codemirror2'
import 'src/external/codemirror'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class TickscriptEditor extends Component {
  constructor(props) {
    super(props)
  }

  updateCode = (_, __, script) => {
    this.props.onChangeScript(script)
  }

  render() {
    const {script} = this.props

    const options = {
      lineNumbers: true,
      theme: 'tickscript',
      tabIndex: 1,
      readonly: false,
      mode: 'tickscript',
    }

    return (
      <div className="tickscript-editor">
        <CodeMirror
          value={script}
          onBeforeChange={this.updateCode}
          options={options}
        />
      </div>
    )
  }
}

const {func, string} = PropTypes

TickscriptEditor.propTypes = {
  onChangeScript: func,
  script: string,
}

export default TickscriptEditor
