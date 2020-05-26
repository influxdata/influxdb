import React, {FC} from 'react'
import {PipeProp} from 'src/notebooks'

const ExampleView: FC<PipeProp> = ({data, Context}) => {
  return (
    <Context>
      <h1>{data.text}</h1>
    </Context>
  )
}

export default ExampleView
