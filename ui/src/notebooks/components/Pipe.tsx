import {FC, createElement, useContext} from 'react'
import {NotebookContext} from 'src/notebooks/context/notebook'

import {PIPE_DEFINITIONS, PipeProp} from 'src/notebooks'

const Pipe: FC<PipeProp> = ({idx}) => {
  const {pipes} = useContext(NotebookContext)

  if (!PIPE_DEFINITIONS.hasOwnProperty(pipes[idx].type)) {
    throw new Error(`Pipe type [${pipes[idx].type}] not registered`)
    return null
  }

  return createElement(PIPE_DEFINITIONS[pipes[idx].type].component, {idx})
}

export default Pipe
