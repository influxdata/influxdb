import React, {FC, useContext} from 'react'
import Pipe from 'src/notebooks/components/Pipe'
import {NotebookContext} from 'src/notebooks/context/notebook'

const PipeList: FC = () => {
  const {id, pipes} = useContext(NotebookContext)
  const _pipes = pipes.map((_, idx) => (
    <Pipe idx={idx} key={`pipe-${id}-${idx}`} />
  ))

  return <>{_pipes}</>
}

export default PipeList
