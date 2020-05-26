import React, {FC, useContext, useCallback} from 'react'
import {NotebookContext} from 'src/notebooks/context/notebook'
import NotebookPipe from 'src/notebooks/components/NotebookPipe'

const PipeList: FC = () => {
  const {id, pipes, updatePipe} = useContext(NotebookContext)
  const update = useCallback(updatePipe, [id])

  const _pipes = pipes.map((_, index) => {
    return (
      <NotebookPipe
        key={`pipe-${id}-${index}`}
        index={index}
        data={pipes[index]}
        onUpdate={update}
      />
    )
  })

  return <>{_pipes}</>
}

export default PipeList
