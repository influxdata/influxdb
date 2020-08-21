// Libraries
import React, {FC} from 'react'
// Types
import {PipeProp} from 'src/notebooks'

// Contexts
import BucketProvider from 'src/notebooks/context/buckets'

// Components
import BucketSelector from 'src/notebooks/pipes/Data/BucketSelector'
import FieldsList from 'src/notebooks/pipes/Data/FieldsList'
import SearchBar from 'src/notebooks/pipes/Data/SearchBar'

// Utils
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

// Styles
import 'src/notebooks/pipes/Query/style.scss'
import {SchemaProvider} from 'src/notebooks/context/schemaProvider'
import FilterTags from './FilterTags'

const DataSource: FC<PipeProp> = ({Context}) => {
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
          <SearchBar />
          {body}
        </Context>
      </SchemaProvider>
    </BucketProvider>
  )
}

export default DataSource
