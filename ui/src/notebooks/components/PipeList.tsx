import React, {FC, useContext} from 'react'
import Pipe from 'src/notebooks/components/Pipe'
import {NotebookContext} from 'src/notebooks/context/notebook'
import NotebookPanel from 'src/notebooks/components/panel/NotebookPanel'

const PipeList: FC = () => {
  const {id, pipes, removePipe, movePipe} = useContext(NotebookContext)
  const _pipes = pipes.map((_, index) => {
    const canBeMovedUp = index > 0
    const canBeMovedDown = index < pipes.length - 1
    const canBeRemoved = index !== 0

    const moveUp = canBeMovedUp ? () => movePipe(index, index - 1) : null
    const moveDown = canBeMovedDown ? () => movePipe(index, index + 1) : null
    const remove = canBeRemoved ? () => removePipe(index) : null

    return (
      <NotebookPanel
        key={`pipe-${id}-${index}`}
        index={index}
        onMoveUp={moveUp}
        onMoveDown={moveDown}
        onRemove={remove}
      >
        <Pipe index={index} />
      </NotebookPanel>
    )
  })

  return <>{_pipes}</>
}

export default PipeList
