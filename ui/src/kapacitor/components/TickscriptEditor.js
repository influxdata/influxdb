import React, {Component} from 'react'
import CodeMirror from 'react-codemirror'
import 'src/external/codemirror'

class TickscriptEditor extends Component {
  constructor(props) {
    super(props)
    this.state = {
      code: '',
    }
  }

  updateCode(code) {
    this.setState({code})
  }

  render() {
    const {code} = this.state

    const options = {
      lineNumbers: true,
      theme: 'material',
    }

    return (
      <CodeMirror value={code} onChange={::this.updateCode} options={options} />
    )
  }
}

export default TickscriptEditor
