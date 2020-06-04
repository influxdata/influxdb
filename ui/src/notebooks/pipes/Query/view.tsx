// Libraries
import React, {FC, useMemo} from 'react'

// Types
import {PipeProp} from 'src/notebooks'
import {ResultsVisibility} from 'src/notebooks/pipes/Query'

// Components
import FluxMonacoEditor from 'src/shared/components/FluxMonacoEditor'
import Results from 'src/notebooks/pipes/Query/Results'

// Styles
import 'src/notebooks/pipes/Query/style.scss'

const Query: FC<PipeProp> = ({data, onUpdate, Context, results}) => {
  const {queries, activeQuery} = data
  const query = queries[activeQuery]
  const resultsVisibility = data.resultsVisibility || 'visible'
  const resultsHeight = data.resultsPanelHeight

  function updateText(text) {
    const _queries = queries.slice()
    _queries[activeQuery] = {
      ...queries[activeQuery],
      text,
    }

    onUpdate({queries: _queries})
  }

  const handleUpdateResultsVisibility = (
    resultsVisibility: ResultsVisibility
  ): void => {
    onUpdate({resultsVisibility})
  }

  const handleUpdateResultsHeight = (resultsPanelHeight: number): void => {
    onUpdate({resultsPanelHeight})
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
        <Results
          results={results}
          height={resultsHeight}
          onUpdateHeight={handleUpdateResultsHeight}
          visibility={resultsVisibility}
          onUpdateVisibility={handleUpdateResultsVisibility}
        />
      </Context>
    ),
    [query.text, results, data.resultsVisibility, data.resultsPanelHeight]
  )
}

export default Query
