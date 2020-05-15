import React, {FC, useContext} from 'react'
import {NotebookContext} from 'src/notebooks/context/notebook'

import Panel from 'src/notebooks/components/Panel'

interface PipeProps {
  idx: number
}

const TITLE = 'Example Pipe'

const ExampleView: FC<PipeProps> = ({idx}) => {
  const {pipes, removePipe} = useContext(NotebookContext)
  const pipe = pipes[idx]

  if (idx) {
    return (
      <Panel onRemove={() => removePipe(idx)} title={TITLE}>
        <h1>{pipe.text}</h1>
      </Panel>
    )
  }

  return (
    <Panel title={TITLE}>
      <h1>{pipe.text}</h1>
    </Panel>
  )
}

export default ExampleView
