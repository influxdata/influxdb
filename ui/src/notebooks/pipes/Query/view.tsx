import React, {FC, useMemo} from 'react'
import {PipeProp} from 'src/notebooks'
import {TimeMachineFluxEditor} from 'src/timeMachine/components/TimeMachineFluxEditor'

const Query: FC<PipeProp> = ({data, onUpdate, Context}) => {
  const {queries, activeQuery} = data
  const query = queries[activeQuery]

  function updateText(text) {
    const _queries = queries.slice()
    _queries[activeQuery] = {
      ...queries[activeQuery],
      text,
    }

    onUpdate({queries: _queries})
  }

  const editor = useMemo(
    () => (
      <TimeMachineFluxEditor
        activeQueryText={query.text}
        activeTab={activeQuery}
        onSetActiveQueryText={updateText}
        onSubmitQueries={() => {}}
      />
    ),
    [query.text]
  )

  return <Context>{editor}</Context>
}

export default Query
