// Libraries
import React, {FC} from 'react'
// Types
import {PipeProp} from 'src/notebooks'

// Contexts
import BucketProvider from 'src/notebooks/context/buckets'

// Components
import BucketSelector from 'src/notebooks/pipes/Data/BucketSelector'
import FieldsList from 'src/notebooks/pipes/Data/FieldsList'

// Utils
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

// Styles
import 'src/notebooks/pipes/Query/style.scss'
import {SchemaProvider} from 'src/notebooks/context/schemaProvider'

const DataSource: FC<PipeProp> = ({Context}) => {
  let body = <span />
  if (isFlagEnabled('flowsQueryBuilder')) {
    body = <FieldsList />
  }
  return (
    <BucketProvider>
      <SchemaProvider>
        <Context controls={<BucketSelector />}>{body}</Context>
      </SchemaProvider>
    </BucketProvider>
  )
}

export default DataSource
