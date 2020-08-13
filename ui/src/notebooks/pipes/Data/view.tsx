// Libraries
import React, {FC} from 'react'
// Types
import {PipeProp} from 'src/notebooks'

// Contexts
import BucketProvider from 'src/notebooks/context/buckets'

// Components
import BucketSelector from 'src/notebooks/pipes/Data/BucketSelector'
import FieldsList from 'src/notebooks/pipes/Data/FieldsList'
import {FlexBox, ComponentSize} from '@influxdata/clockface'

// Styles
import 'src/notebooks/pipes/Query/style.scss'
import {SchemaProvider} from 'src/notebooks/context/schemaProvider'

const DataSource: FC<PipeProp> = ({Context}) => {
  return (
    <BucketProvider>
      <SchemaProvider>
        <Context>
          <FlexBox
            margin={ComponentSize.Large}
            stretchToFitWidth={true}
            className="data-source"
          >
            <BucketSelector />
            <FieldsList />
          </FlexBox>
        </Context>
      </SchemaProvider>
    </BucketProvider>
  )
}

export default DataSource
