// Libraries
import React, {FC} from 'react'

// Types
import {PipeProp} from 'src/notebooks'

// Components
import BucketSelector from 'src/notebooks/pipes/Bucket/BucketSelector'
import {FlexBox, ComponentSize} from '@influxdata/clockface'
import BucketProvider from 'src/notebooks/context/buckets'

// Styles
import 'src/notebooks/pipes/Query/style.scss'

const DataSource: FC<PipeProp> = ({Context}) => {
  return (
    <BucketProvider>
      <Context>
        <FlexBox
          margin={ComponentSize.Large}
          stretchToFitWidth={true}
          className="data-source"
        >
          <BucketSelector />
        </FlexBox>
      </Context>
    </BucketProvider>
  )
}

export default DataSource
