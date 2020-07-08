// Libraries
import React, {FC, useMemo, useContext} from 'react'

// Types
import {PipeProp} from 'src/notebooks'

// Components
import BucketSelector from 'src/notebooks/pipes/Data/BucketSelector'
import TimeSelector from 'src/notebooks/pipes/Data/TimeSelector'
import {FlexBox, ComponentSize} from '@influxdata/clockface'
import BucketProvider from 'src/notebooks/context/buckets'
import {PipeContext} from 'src/notebooks/context/pipe'

// Styles
import 'src/notebooks/pipes/Query/style.scss'

const DataSource: FC<PipeProp> = ({Context}) => {
  const {data} = useContext(PipeContext)

  return useMemo(
    () => (
      <BucketProvider>
        <Context>
          <FlexBox
            margin={ComponentSize.Large}
            stretchToFitWidth={true}
            className="data-source"
          >
            <BucketSelector />
            <TimeSelector />
          </FlexBox>
        </Context>
      </BucketProvider>
    ),
    [data.bucketName, data.timeStart, data.timeStop]
  )
}

export default DataSource
