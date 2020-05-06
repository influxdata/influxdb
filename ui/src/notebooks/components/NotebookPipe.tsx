import React, {FC, createElement, useContext} from 'react'
import {NotebookContext} from 'src/notebooks/notebook.context'

import {PIPE_DEFINITIONS} from 'src/notebooks'

interface PipeProps {
  idx: number
}

const NotebookPipe: FC<PipeProps> = ({idx}) => {
  const {pipes} = useContext(NotebookContext)

  if (!PIPE_DEFINITIONS.hasOwnProperty(pipes[idx].type)) {
    throw new Error(`NotebookPipe type [${type}] not registered`)
    return null
  }

  return createElement(PIPE_DEFINITIONS[pipes[idx].type].component, {idx})
}

export default NotebookPipe
