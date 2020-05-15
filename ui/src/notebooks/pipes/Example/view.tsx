import React, {FC, useContext} from 'react'
import {PipeProp} from 'src/notebooks'
import {NotebookContext} from 'src/notebooks/context/notebook'

import Panel from 'src/notebooks/components/Panel'

const TITLE = 'Example Pipe'

const ExampleView: FC<PipeProp> = ({index}) => {
  const {pipes, removePipe} = useContext(NotebookContext)
  const pipe = pipes[index]

  if (index) {
    return (
      <Panel onRemove={() => removePipe(index)} title={TITLE}>
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
