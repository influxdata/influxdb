import React, {Component} from 'react'
import MonacoEditor from 'react-monaco-editor'

class TickscriptEditor extends Component {
  constructor(props) {
    super(props)
    this.state = {
      code: '',
    }
  }

  editorDidMount(editor, monaco) {
    console.log('editorDidMount', editor, monaco)
    editor.focus()
  }

  onChange(newValue, e) {
    console.log('onChange', newValue, e)
  }

  render() {
    const {code} = this.state

    const options = {
      selectOnLineNumbers: true,
    }

    return (
      <MonacoEditor
        width="800"
        height="600"
        language="javascript"
        value={code}
        options={options}
        onChange={::this.onChange}
        editorDidMount={::this.editorDidMount}
      />
    )
  }
}

export default TickscriptEditor
