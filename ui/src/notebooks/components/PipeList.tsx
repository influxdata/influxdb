import React, {FC, useContext, createElement} from 'react'
import {PipeContextProps, PipeData} from 'src/notebooks'
import Pipe from 'src/notebooks/components/Pipe'
import {NotebookContext} from 'src/notebooks/context/notebook'
import NotebookPanel from 'src/notebooks/components/panel/NotebookPanel'

const PipeList: FC = () => {
  const {id, pipes, updatePipe} = useContext(NotebookContext)
  const _pipes = pipes.map((pipe, index) => {
      const panel: FC<PipeContextProps> = (props) => {
          const _props = {
              ...props,
              index
          }

          return createElement(NotebookPanel, _props)
      }
    const onUpdate = (data: PipeData) => {
        updatePipe(index, data)
    }

    return (
      <Pipe
        key={`pipe-${id}-${index}`}
        data={pipe}
        onUpdate={onUpdate}
        Context={panel}
      />
    )
  })

  return <>{_pipes}</>
}

export default PipeList
