// Libraries
import React, {FC} from 'react'
import Editor from '@monaco-editor/react'
import 'src/external/monaco.flux'

interface Props {
  script: string
}

const FluxEditorMonaco: FC<Props> = ({script}) => {
  return (
    <div className="time-machine-editor" data-testid="flux-editor">
      <Editor height="90vh" language="Flux" value={script} />
    </div>
  )
}

export default FluxEditorMonaco
