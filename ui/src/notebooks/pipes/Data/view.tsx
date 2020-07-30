// Libraries
import React, {FC} from 'react'

// Types
import {PipeProp} from 'src/notebooks'

// Contexts
import BucketProvider from 'src/notebooks/context/buckets'

// Components
import BucketSelector from 'src/notebooks/pipes/Data/BucketSelector'
import Schema from 'src/notebooks/pipes/Data/Schema'
import {FlexBox, ComponentSize} from '@influxdata/clockface'

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
          <Schema />
        </FlexBox>
      </Context>
    </BucketProvider>
  )
}

export default DataSource
