// Libraries
import React, {FC, useMemo, useContext} from 'react'

// Types
import {PipeProp} from 'src/notebooks'

// Components
import FluxMonacoEditor from 'src/shared/components/FluxMonacoEditor'
import Results from 'src/notebooks/pipes/Query/Results'
import {PipeContext} from 'src/notebooks/context/pipe'

// Styles
import 'src/notebooks/pipes/Query/style.scss'

const Query: FC<PipeProp> = ({Context}) => {
  const {data, update, results} = useContext(PipeContext)
  const {queries, activeQuery} = data
  const query = queries[activeQuery]

  function updateText(text) {
    const _queries = queries.slice()
    _queries[activeQuery] = {
      ...queries[activeQuery],
      text,
    }

    update({queries: _queries})
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
        <Results />
      </Context>
    ),
    [query.text, results, data.panelVisibility, data.panelHeight]
  )
}

export default Query
