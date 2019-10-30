// Libraries
import React, {FC} from 'react'
import 'src/external/monaco.fluxLang'
import 'src/external/monaco.fluxTheme'
import 'src/external/monaco.fluxCompletions'

// Components
import Editor from '@monaco-editor/react'

interface Props {
  script: string
}

const FluxEditorMonaco: FC<Props> = ({script}) => {
  return (
    <div className="time-machine-editor" data-testid="flux-editor">
      <Editor height="90vh" language="flux" value={script} />
    </div>
  )
}

export default FluxEditorMonaco
