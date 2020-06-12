// Libraries
import React, {FC, useMemo} from 'react'

// Types
import {PipeProp} from 'src/notebooks'

// Components
import FluxMonacoEditor from 'src/shared/components/FluxMonacoEditor'
import Results from 'src/notebooks/pipes/Query/Results'

// Styles
import 'src/notebooks/pipes/Query/style.scss'

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
        <Results results={results} onUpdate={onUpdate} data={data} />
      </Context>
    ),
    [query.text, results, data.panelVisibility, data.panelHeight]
  )
}

export default Query
