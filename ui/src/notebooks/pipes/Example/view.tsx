import React, {FC, useContext} from 'react'
import {PipeProp} from 'src/notebooks'
import {NotebookContext} from 'src/notebooks/context/notebook'

const ExampleView: FC<PipeProp> = ({index, contextInteraction}) => {
  const {pipes} = useContext(NotebookContext)
  const pipe = pipes[index]

  return (
    <div className="pipe-example">
      {contextInteraction}
      <h1>{pipe.text}</h1>
    </div>
  )
}

export default ExampleView
