import React, {FC, useContext} from 'react'
import {PipeProp} from 'src/notebooks'
import {NotebookContext} from 'src/notebooks/context/notebook'
import {TimeMachineFluxEditor} from 'src/timeMachine/components/TimeMachineFluxEditor'

const Query: FC<PipeProp> = ({index}) => {
  const {pipes, updatePipe} = useContext(NotebookContext)
  const {queries, activeQuery} = pipes[index]
  const query = queries[activeQuery]

  function updateText(text) {
    const _queries = queries.slice()
    _queries[activeQuery] = {
      ...queries[activeQuery],
      text,
    }

    updatePipe(index, {queries: _queries})
  }

  const queryPipes = pipes
    .map(({type}, index) => ({type, index}))
    .filter(pipe => pipe.type === 'query')
  const isLast = queryPipes[queryPipes.length - 1].index === index

  return (
    <div className="notebook-query">
      <TimeMachineFluxEditor
        activeQueryText={query.text}
        activeTab={activeQuery}
        onSetActiveQueryText={updateText}
        onSubmitQueries={() => {}}
        skipFocus={!isLast}
      />
    </div>
  )
}

export default Query
