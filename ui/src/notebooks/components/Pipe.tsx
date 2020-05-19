import {FC, createElement, useContext} from 'react'
import {NotebookContext} from 'src/notebooks/context/notebook'

import {PIPE_DEFINITIONS, PipeProp} from 'src/notebooks'

const Pipe: FC<PipeProp> = props => {
  const {index} = props
  const {pipes} = useContext(NotebookContext)

  if (!PIPE_DEFINITIONS.hasOwnProperty(pipes[index].type)) {
    throw new Error(`Pipe type [${pipes[index].type}] not registered`)
    return null
  }

  return createElement(PIPE_DEFINITIONS[pipes[index].type].component, props)
}

export default Pipe
