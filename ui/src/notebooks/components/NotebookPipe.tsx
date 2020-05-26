import React, {FC, createElement, useMemo} from 'react'

import {PipeContextProps, PipeData} from 'src/notebooks'
import Pipe from 'src/notebooks/components/Pipe'
import NotebookPanel from 'src/notebooks/components/panel/NotebookPanel'

export interface NotebookPipeProps {
  index: number
  data: PipeData
  onUpdate: (index: number, pipe: PipeData) => void
}

const NotebookPipe: FC<NotebookPipeProps> = ({index, data, onUpdate}) => {
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

  return <Pipe data={data} onUpdate={_onUpdate} Context={panel} />
}

export default NotebookPipe
