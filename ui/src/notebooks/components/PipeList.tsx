import React, {FC, useContext} from 'react'
import Pipe from 'src/notebooks/components/Pipe'
import {NotebookContext} from 'src/notebooks/context/notebook'
import NotebookPanel from 'src/notebooks/components/panel/NotebookPanel'

const PipeList: FC = () => {
  const {id, pipes, meta} = useContext(NotebookContext)
  const _pipes = pipes.map((_, index) => {
    const header = <NotebookPanel index={index} />

    if (!meta[index].visible) {
      return (
        <div key={`pipe-${id}-${index}`} className="panel-empty">
          {header}
        </div>
      )
    }

    return (
      <Pipe
        index={index}
        key={`pipe-${id}-${index}`}
        contextInteraction={header}
      />
    )
  })

  return <>{_pipes}</>
}

export default PipeList
