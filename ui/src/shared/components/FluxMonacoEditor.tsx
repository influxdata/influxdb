// Libraries
import React, {FC} from 'react'

// Components
import MonacoEditor from 'react-monaco-editor'
import {tokenizeFlux} from 'src/external/monaco.fluxLang'
import {addFluxTheme} from 'src/external/monaco.fluxTheme'
import {addSnippets} from 'src/external/monaco.fluxCompletions'

interface Props {
  script: string
}

const FluxEditorMonaco: FC<Props> = ({script}) => {
  const editorWillMount = monaco => {
    tokenizeFlux(monaco)
    addFluxTheme(monaco)
    addSnippets(monaco)
  }

  return (
    <div className="time-machine-editor" data-testid="flux-editor">
      <MonacoEditor
        width="800"
        height="600"
        language="flux"
        theme="vs-light"
        value={script}
        editorWillMount={editorWillMount}
      />
    </div>
  )
}

export default FluxEditorMonaco
