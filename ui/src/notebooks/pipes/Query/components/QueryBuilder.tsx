import React, {FC, useContext} from 'react'
import {buildQuery} from 'src/timeMachine/utils/queryBuilder'
import {TimeMachineQueryBuilder} from 'src/timeMachine/components/QueryBuilder'
import {NotebookContext} from 'src/notebooks/notebook.context'

interface Props {
    idx: number
}

const QueryBuilder: FC = ({idx}) => {
  const {pipes, updatePipe, removePipe} = useContext(NotebookContext)
  const {queries, activeQuery} = pipes[idx]
  const query = queries[activeQuery]

  if (query.editMode !== 'builder') {
      return null
  }

  return (
    <TimeMachineQueryBuilder
        tagFiltersLength={ query.builderConfig.tags.length }
        checkType={ "not a real type" }
        moreTags={ false }
        onLoadBuckets={ () => {} }
        onAddTagSelector={ () => {} }
    />
  )
}

export default QueryBuilder
