import React, {FC, useContext} from 'react'
import {PipeProp} from 'src/notebooks'
import {NotebookContext} from 'src/notebooks/context/notebook'

const ExampleView: FC<PipeProp> = ({index}) => {
  const {pipes} = useContext(NotebookContext)
  const pipe = pipes[index]

  return (
    <>
      <h1>{pipe.text}</h1>
    </>
  )
}

export default ExampleView
