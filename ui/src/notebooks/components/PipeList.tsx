import React, {FC, useContext} from 'react'
import Pipe from 'src/notebooks/components/Pipe'
import {NotebookContext} from 'src/notebooks/context/notebook'

const PipeList: FC = () => {
  const {id, pipes} = useContext(NotebookContext)
  const _pipes = pipes.map((_, index) => (
    <Pipe index={index} key={`pipe-${id}-${index}`} />
  ))

  return <>{_pipes}</>
}

export default PipeList
