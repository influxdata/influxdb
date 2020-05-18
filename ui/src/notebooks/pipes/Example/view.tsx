import React, {FC, useContext} from 'react'
import {PipeProp} from 'src/notebooks'
import {NotebookContext} from 'src/notebooks/context/notebook'

import NotebookPanel from 'src/notebooks/components/panel/NotebookPanel'

const TITLE = 'Example Pipe'

const ExampleView: FC<PipeProp> = ({index, remove, moveUp, moveDown}) => {
  const {pipes} = useContext(NotebookContext)
  const pipe = pipes[index]

  if (index) {
    return (
      <NotebookPanel
        id={`pipe${index}`}
        onRemove={() => remove()}
        onMoveUp={() => moveUp()}
        onMoveDown={() => moveDown()}
        title={TITLE}
      >
        <h1>{pipe.text}</h1>
      </NotebookPanel>
    )
  }

  return (
    <NotebookPanel
      id={`pipe${index}`}
      onMoveUp={() => moveUp()}
      onMoveDown={() => moveDown()}
      title={TITLE}
    >
      <h1>{pipe.text}</h1>
    </NotebookPanel>
  )
}

export default ExampleView
