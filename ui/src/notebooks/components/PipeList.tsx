import React, {FC, useContext, useCallback, createElement, useMemo} from 'react'
import {PipeContextProps, PipeData} from 'src/notebooks'
import Pipe from 'src/notebooks/components/Pipe'
import {NotebookContext} from 'src/notebooks/context/notebook'
import NotebookPanel from 'src/notebooks/components/panel/NotebookPanel'

interface NotebookPipeProps {
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
