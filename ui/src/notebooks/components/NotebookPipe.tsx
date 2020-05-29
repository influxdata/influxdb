import React, {FC, createElement, useMemo} from 'react'

import {PipeContextProps, PipeData, PipeProp} from 'src/notebooks'
import Pipe from 'src/notebooks/components/Pipe'
import NotebookPanel from 'src/notebooks/components/panel/NotebookPanel'

export interface NotebookPipeProps
  extends Omit<Omit<PipeProp, 'Context'>, 'onUpdate'> {
  index: number
  onUpdate: (idx: number, data: PipeData) => void
}

const NotebookPipe: FC<NotebookPipeProps> = ({
  index,
  data,
  onUpdate,
  results,
}) => {
  const panel: FC<PipeContextProps> = useMemo(
    () => props => {
      const _props = {
        ...props,
        index,
      }

      return createElement(NotebookPanel, _props)
    },
    [index]
  )

  const _onUpdate = (data: PipeData) => {
    onUpdate(index, data)
  }

  return (
    <Pipe data={data} onUpdate={_onUpdate} results={results} Context={panel} />
  )
}

export default NotebookPipe
