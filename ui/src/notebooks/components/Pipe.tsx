import {FC, createElement, useContext} from 'react'

import {PIPE_DEFINITIONS, PipeProp} from 'src/notebooks'
import {PipeContext} from 'src/notebooks/context/pipe'

const Pipe: FC<PipeProp> = props => {
  const {data} = useContext(PipeContext)

  if (!PIPE_DEFINITIONS.hasOwnProperty(data.type)) {
    throw new Error(`Pipe type [${data.type}] not registered`)
    return null
  }

  return createElement(PIPE_DEFINITIONS[data.type].component, props)
}

export default Pipe
