import React, {FC, useContext} from 'react'
import {DashboardQuery} from 'src/client'
import {TimeMachineFluxEditor} from 'src/timeMachine/components/TimeMachineFluxEditor'
import {TimeMachineQueriesSwitcher} from 'src/timeMachine/components/QueriesSwitcher'
import {NotebookContext} from 'src/notebooks/notebook.context'
import NotebookPanel from 'src/notebooks/components/NotebookPanel'
import {buildQuery} from 'src/timeMachine/utils/queryBuilder'

export interface Props {
  queries: DashboardQuery[]
  activeQuery: number
  onUpdate: (idx: number, query: DashboardQuery) => void
}

const QueryView: FC = ({idx}) => {
  const {pipes, updatePipe, removePipe} = useContext(NotebookContext)
  const {queries, activeQuery} = pipes[idx]

  function updateText(text) {
    const _queries = queries.slice()
    _queries[activeQuery] = {
      ...queries[activeQuery],
      text,
    }

    updatePipe(idx, {queries: _queries})
  }

  function editWithBuilder() {
    const _queries = queries.slice()
    _queries[activeQuery] = {
      ...queries[activeQuery],
      text: buildQuery(queries[activeQuery].builderConfig),
      editMode: 'builder',
    }

    updatePipe(idx, {queries: _queries})
  }

  function editAsFlux() {
    const _queries = queries.slice()
    _queries[activeQuery] = {
      ...queries[activeQuery],
      text: buildQuery(queries[activeQuery].builderConfig),
      editMode: 'advanced',
    }

    updatePipe(idx, {queries: _queries})
  }

  const switcher = (
    <TimeMachineQueriesSwitcher
      activeQuery={queries[activeQuery]}
      onEditWithBuilder={editWithBuilder}
      onEditAsFlux={editAsFlux}
    />
  )

  return (
    <NotebookPanel
      onRemove={
        idx
          ? () => {
              removePipe(idx)
            }
          : false
      }
      title="Query Editor"
      controlsLeft={switcher}
    >
      {queries[activeQuery].editMode === 'advanced' && (
        <TimeMachineFluxEditor
          activeQueryText={queries[activeQuery].text}
          activeTab={activeQuery}
          onSetActiveQueryText={updateText}
          onSubmitQueries={() => {}}
        />
      )}
    </NotebookPanel>
  )
}

export default QueryView
