import React, {FC, useContext} from 'react'
import {TimeMachineFluxEditor} from 'src/timeMachine/components/TimeMachineFluxEditor'
import {NotebookContext} from 'src/notebooks/notebook.context'

interface Props {
    idx: number
}

const FluxEditor: FC<Props> = ({idx}) => {
  const {pipes, updatePipe} = useContext(NotebookContext)
  const {queries, activeQuery} = pipes[idx]
  const query = queries[activeQuery]

  function updateText(text) {
    const _queries = queries.slice()
    _queries[activeQuery] = {
      ...queries[activeQuery],
      text,
    }

    updatePipe(idx, {queries: _queries})
  }

  if (query.editMode !== 'advanced') {
      return null
  }

  return (
    <TimeMachineFluxEditor
      activeQueryText={query.text}
      activeTab={activeQuery}
      onSetActiveQueryText={updateText}
      onSubmitQueries={() => {}}
    />
  )
}

export default FluxEditor
