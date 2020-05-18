// Libraries
import React, {FC, useContext} from 'react'

// Components
import {Button, ComponentColor} from '@influxdata/clockface'

// Constants
import {NotebookContext} from 'src/notebooks/context/notebook'
import {PIPE_DEFINITIONS} from 'src/notebooks'

const AddButtons: FC = () => {
  const {addPipe} = useContext(NotebookContext)

  const pipes = Object.entries(PIPE_DEFINITIONS).map(([type, def]) => {
    return (
      <Button
        key={def.type}
        text={def.button}
        onClick={() => {
          let data = def.empty
          if (typeof data === 'function') {
            data = data()
          }
          addPipe({
            ...data,
            type,
          })
        }}
        color={ComponentColor.Secondary}
      />
    )
  })

  return <>{pipes}</>
}

export default AddButtons
