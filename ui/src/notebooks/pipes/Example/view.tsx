import React, {FC, createElement, useContext} from 'react'
import {NotebookContext} from 'src/notebooks/context/notebook'

import Panel from 'src/notebooks/components/Panel'

interface PipeProps {
  idx: number
}

const ExampleView: FC<PipeProps> = ({idx}) => {
  const {pipes, removePipe} = useContext(NotebookContext)
  const pipe = pipes[idx]
  const remove = idx ? () => removePipe(idx) : false

  return (
    <Panel onRemove={remove} title="Example Pipe">
      <h1>{pipe.text}</h1>
    </Panel>
  )
}

export default ExampleView
