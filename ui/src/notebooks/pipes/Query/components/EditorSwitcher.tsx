import React, {FC, useContext} from 'react'
import {buildQuery} from 'src/timeMachine/utils/queryBuilder'
import {TimeMachineQueriesSwitcher} from 'src/timeMachine/components/QueriesSwitcher'
import {NotebookContext} from 'src/notebooks/notebook.context'

interface Props {
    idx: number
}

const EditorSwitcher: FC = ({idx}) => {
  const {pipes, updatePipe, removePipe} = useContext(NotebookContext)
  const {queries, activeQuery} = pipes[idx]
  const query = queries[activeQuery]

  function editWithBuilder() {
    const _queries = queries.slice()
    _queries[activeQuery] = {
      ...query,
      text: buildQuery(query.builderConfig),
      editMode: 'builder',
    }

    updatePipe(idx, {queries: _queries})
  }

  function editAsFlux() {
    const _queries = queries.slice()
    _queries[activeQuery] = {
      ...query,
      text: buildQuery(query.builderConfig),
      editMode: 'advanced',
    }

    updatePipe(idx, {queries: _queries})
  }

  return (
    <TimeMachineQueriesSwitcher
      activeQuery={query}
      onEditWithBuilder={editWithBuilder}
      onEditAsFlux={editAsFlux}
    />
  )
}

export default EditorSwitcher
