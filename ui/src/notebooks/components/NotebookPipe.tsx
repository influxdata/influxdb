import React, {FC, createElement, useMemo} from 'react'

import {PipeContextProps, PipeData, DataID} from 'src/notebooks'
import Pipe from 'src/notebooks/components/Pipe'
import NotebookPanel from 'src/notebooks/components/panel/NotebookPanel'
import {PipeProvider} from 'src/notebooks/context/pipe'

export interface NotebookPipeProps {
  id: DataID<PipeData>
}

const NotebookPipe: FC<NotebookPipeProps> = ({id}) => {
  const panel: FC<PipeContextProps> = useMemo(
    () => props => {
      const _props = {
        ...props,
        id,
      }

      return createElement(NotebookPanel, _props)
    },
    [id]
  )

  return (
    <PipeProvider id={id}>
      <Pipe Context={panel} />
    </PipeProvider>
  )
}

export default NotebookPipe
