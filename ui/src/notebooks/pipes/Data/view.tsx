// Libraries
import React, {FC} from 'react'
// Types
import {PipeProp} from 'src/notebooks'

// Contexts
import BucketProvider from 'src/notebooks/context/buckets'

// Components
import BucketSelector from 'src/notebooks/pipes/Data/BucketSelector'
import FieldsList from 'src/notebooks/pipes/Data/FieldsList'

// Styles
import 'src/notebooks/pipes/Query/style.scss'
import {SchemaProvider} from 'src/notebooks/context/schemaProvider'

const DataSource: FC<PipeProp> = ({Context}) => {
  return (
    <BucketProvider>
      <SchemaProvider>
        <Context controls={<BucketSelector />}>
          <FieldsList />
        </Context>
      </SchemaProvider>
    </BucketProvider>
  )
}

export default DataSource
