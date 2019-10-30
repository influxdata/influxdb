// Libraries
import React, {FC} from 'react'
import 'src/external/codemirror'
import 'codemirror/addon/comment/comment'
import Editor from '@monaco-editor/react'

interface Props {
  script: string
}

const MonacoEditor: FC<Props> = ({script}) => {
  return (
    <div className="time-machine-editor" data-testid="flux-editor">
      <Editor height="90vh" language="javascript" value={script} />
    </div>
  )
}

export default MonacoEditor
