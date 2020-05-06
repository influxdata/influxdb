import React, {FC, useContext} from 'react'
import {Button, ComponentColor} from '@influxdata/clockface'
import {NotebookContext} from 'src/notebooks/notebook.context'
import {PIPE_DEFINITIONS} from 'src/notebooks'

const NotebookButtons: FC = () => {
  const {addPipe} = useContext(NotebookContext)

  const pipes = Object.entries(PIPE_DEFINITIONS).map(([type, def]) => {
    return (
        <Button key={ def.type }
            text={ def.button }
            onClick={ () => {
          addPipe({
            ...def.empty,
            type,
          })
            }}
            color={ComponentColor.Secondary} />
    )
  })

  return <>{pipes}</>
}

export default NotebookButtons
