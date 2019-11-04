// Libraries
import React, {FC} from 'react'

// Components
import MonacoEditor from 'react-monaco-editor'

interface Props {
  script: string
}

const FluxEditorMonaco: FC<Props> = ({script}) => {
  return (
    <div className="time-machine-editor" data-testid="flux-editor">
      <MonacoEditor
        width="800"
        height="600"
        language="javascript"
        theme="vs-light"
        value={script}
      />
    </div>
  )
}

export default FluxEditorMonaco
