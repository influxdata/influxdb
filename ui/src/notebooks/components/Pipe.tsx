import {FC, createElement} from 'react'

import {PIPE_DEFINITIONS, PipeProp} from 'src/notebooks'

const Pipe: FC<PipeProp> = props => {
  const {data} = props

  if (!PIPE_DEFINITIONS.hasOwnProperty(data.type)) {
    throw new Error(`Pipe type [${data.type}] not registered`)
    return null
  }

  return createElement(PIPE_DEFINITIONS[data.type].component, props)
}

export default Pipe
