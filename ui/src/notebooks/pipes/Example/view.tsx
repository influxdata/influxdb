import React, {FC, useContext} from 'react'
import {PipeProp} from 'src/notebooks'
import {NotebookContext} from 'src/notebooks/context/notebook'

import {NotebookPanel} from 'src/notebooks/components/Notebook'

const TITLE = 'Example Pipe'

const ExampleView: FC<PipeProp> = ({index, remove, moveUp, moveDown}) => {
  const {pipes} = useContext(NotebookContext)
  const pipe = pipes[index]

  const canBeMovedUp = index > 0
  const canBeMovedDown = index < pipes.length - 1
  const canBeRemoved = index !== 0

  return (
    <NotebookPanel
      id={`pipe${index}`}
      onMoveUp={canBeMovedUp && moveUp}
      onMoveDown={canBeMovedDown && moveDown}
      onRemove={canBeRemoved && remove}
      title={TITLE}
    >
      <h1>{pipe.text}</h1>
    </NotebookPanel>
  )
}

export default ExampleView
