import React, {FC, useContext} from 'react'
import {NotebookContext} from 'src/notebooks/notebook.context'
import NotebookPanel from 'src/notebooks/components/NotebookPanel'
import FluxEditor from './FluxEditor'
import QueryBuilder from './QueryBuilder'
import EditorSwitcher from './EditorSwitcher'

interface Props {
    idx: number
}

const QueryView: FC = ({idx}) => {
  const {removePipe} = useContext(NotebookContext)

  const remove = idx ? () => removePipe(idx) : false
  const switcher = (
      <EditorSwitcher idx={ idx }/>
  )

  return (
    <NotebookPanel
      onRemove={ remove }
      title="Query Editor"
      controlsLeft={switcher}
    >
        <FluxEditor idx={ idx }/>
        <QueryBuilder idx={ idx } />
    </NotebookPanel>
  )
}

export default QueryView
