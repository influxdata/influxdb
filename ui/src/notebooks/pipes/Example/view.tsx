import React, {FC, useContext} from 'react'
import {PipeProp} from 'src/notebooks'
import {NotebookContext} from 'src/notebooks/context/notebook'

import {NotebookPanel} from 'src/notebooks/components/Notebook'

const TITLE = 'Example Pipe'

const ExampleView: FC<PipeProp> = ({index}) => {
  const {pipes, removePipe} = useContext(NotebookContext)
  const pipe = pipes[index]

  if (index) {
    return (
      <NotebookPanel
        id={`pipe${index}`}
        onRemove={() => removePipe(index)}
        title={TITLE}
      >
        <h1>{pipe.text}</h1>
      </NotebookPanel>
    )
  }

  return (
    <NotebookPanel id={`pipe${index}`} title={TITLE}>
      <h1>{pipe.text}</h1>
    </NotebookPanel>
  )
}

export default ExampleView
