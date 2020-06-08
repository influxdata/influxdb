// Libraries
import React, {FC, useMemo} from 'react'

// Types
import {PipeProp} from 'src/notebooks'
import {RawDataSize} from 'src/notebooks/pipes/Query'

// Components
import FluxMonacoEditor from 'src/shared/components/FluxMonacoEditor'
import Results from 'src/notebooks/pipes/Query/Results'

// Styles
import 'src/notebooks/pipes/Query/style.scss'

const Query: FC<PipeProp> = ({data, onUpdate, Context, results}) => {
  const {queries, activeQuery} = data
  const query = queries[activeQuery]
  const size = data.rawDataSize || 'small'

  function updateText(text) {
    const _queries = queries.slice()
    _queries[activeQuery] = {
      ...queries[activeQuery],
      text,
    }

    onUpdate({queries: _queries})
  }

  const onUpdateSize = (rawDataSize: RawDataSize): void => {
    onUpdate({rawDataSize})
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
        <Results results={results} size={size} onUpdateSize={onUpdateSize} />
      </Context>
    ),
    [query.text, results, data.rawDataSize]
  )
}

export default Query
