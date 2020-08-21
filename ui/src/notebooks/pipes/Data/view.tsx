// Libraries
import React, {FC, useCallback, useContext} from 'react'
import {Input} from '@influxdata/clockface'
// Types
import {PipeProp} from 'src/notebooks'

// Contexts
import BucketProvider from 'src/notebooks/context/buckets'

// Components
import BucketSelector from 'src/notebooks/pipes/Data/BucketSelector'
import FieldsList from 'src/notebooks/pipes/Data/FieldsList'
import {SchemaContext} from 'src/notebooks/context/schemaProvider'

// Utils
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

// Styles
import 'src/notebooks/pipes/Query/style.scss'
import {SchemaProvider} from 'src/notebooks/context/schemaProvider'
import {PipeContext} from 'src/notebooks/context/pipe'
import FilterTags from './FilterTags'

const DataSource: FC<PipeProp> = ({Context}) => {
  const {data} = useContext(PipeContext)
  const {localFetchSchema} = useContext(SchemaContext)

  const selectedBucketName = data.bucketName

  const handleClick = useCallback(() => localFetchSchema(selectedBucketName), [
    localFetchSchema,
    selectedBucketName,
  ])
  const {searchTerm, setSearchTerm} = useContext(SchemaContext)
  let body = <span />
  if (isFlagEnabled('flowsQueryBuilder')) {
    body = <FieldsList />
  }
  return (
    <BucketProvider>
      <SchemaProvider>
        <Context>
          <div className="data-source--controls">
            <BucketSelector />
            <FilterTags />
          </div>
          <Input
            value={searchTerm}
            placeholder="Type to filter by Measurement, Field, or Tag ..."
            className="tag-selector--search"
            onChange={e => setSearchTerm(e.target.value)}
          />
          {body}
        </Context>
      </SchemaProvider>
    </BucketProvider>
  )
}

export default DataSource
