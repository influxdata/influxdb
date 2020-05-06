import React, {FC, useContext} from 'react'
import {NotebookContext} from 'src/notebooks/notebook.context'
import NotebookPanel from 'src/notebooks/components/NotebookPanel'
import EmptyQuery from './EmptyQuery'
import {RemoteDataState} from 'src/types'

interface Props {
    idx: number
}

const Visualization: FC<Props> = ({idx}) => {
  const {removePipe} = useContext(NotebookContext)

  const remove = idx ? () => removePipe(idx) : false

  return (
    <NotebookPanel
      onRemove={ remove }
      title="Visualization"
    >
        <EmptyQuery
          status={RemoteDataState.NotStarted}
          hasResults={false}
        />
    </NotebookPanel>
  )
}

export default Visualization
