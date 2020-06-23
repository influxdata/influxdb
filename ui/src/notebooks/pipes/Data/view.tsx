// Libraries
import React, {FC, useMemo} from 'react'

// Types
import {PipeProp} from 'src/notebooks'

// Components
import BucketSelector from 'src/notebooks/pipes/Data/BucketSelector'
import TimeSelector from 'src/notebooks/pipes/Data/TimeSelector'
import {FlexBox, ComponentSize} from '@influxdata/clockface'

// Styles
import 'src/notebooks/pipes/Query/style.scss'

const DataSource: FC<PipeProp> = ({data, onUpdate, Context}) => {
  return useMemo(
    () => (
      <Context>
        <FlexBox
          margin={ComponentSize.Large}
          stretchToFitWidth={true}
          className="data-source"
        >
          <BucketSelector onUpdate={onUpdate} data={data} />
          <TimeSelector onUpdate={onUpdate} data={data} />
        </FlexBox>
      </Context>
    ),
    [data.bucketName, data.timeStart, data.timeStop]
  )
}

export default DataSource
