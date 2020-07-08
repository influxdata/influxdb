import {FC, createElement, useMemo, useContext} from 'react'

import {PIPE_DEFINITIONS, PipeProp} from 'src/notebooks'
import {PipeContext} from 'src/notebooks/context/pipe'

const Pipe: FC<PipeProp> = props => {
  const {data, results} = useContext(PipeContext)

  if (!PIPE_DEFINITIONS.hasOwnProperty(data.type)) {
    throw new Error(`Pipe type [${data.type}] not registered`)
    return null
  }

  return useMemo(
    () => createElement(PIPE_DEFINITIONS[data.type].component, props),
    [data, results]
  )
}

export default Pipe
