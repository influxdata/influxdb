// Libraries
import React, {FC, useMemo} from 'react'
import {get} from 'lodash'

// Types
import {PipeProp} from 'src/notebooks'

// Components
import FluxMonacoEditor from 'src/shared/components/FluxMonacoEditor'
import Results from 'src/notebooks/pipes/Query/Results'

const Query: FC<PipeProp> = ({data, onUpdate, Context, results}) => {
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

  return useMemo(
    () => (
      <Context>
        <FluxMonacoEditor
          script={query.text}
          onChangeScript={updateText}
          onSubmitScript={() => {}}
          autogrow
        />
      </Context>
    ),
    [query.text]
  )
}

export default Query
