// Libraries
import React, {FC, useEffect, useContext} from 'react'

// Components
import {
  DapperScrollbars,
  TechnoSpinner,
  ComponentSize,
} from '@influxdata/clockface'
import SelectorListItem from 'src/notebooks/pipes/Data/SelectorListItem'
import {BucketContext} from 'src/notebooks/context/buckets'

// Types
import {PipeData} from 'src/notebooks'
import {Bucket} from 'src/types'

interface Props {
  onUpdate: (data: any) => void
  data: PipeData
}

const BucketSelector: FC<Props> = ({onUpdate, data}) => {
  const bucketName = data.bucketName
  const {buckets} = useContext(BucketContext)

  const updateBucket = (updatedBucket: Bucket): void => {
    onUpdate({bucketName: updatedBucket.name})
  }

  useEffect(() => {
    if (!!buckets.length && !bucketName) {
      updateBucket(buckets[0])
    }
  }, [buckets])

  let body = (
    <div className="data-source--list__empty">
      <TechnoSpinner strokeWidth={ComponentSize.Small} diameterPixels={32} />
    </div>
  )

  if (buckets.length && bucketName) {
    body = (
      <DapperScrollbars className="data-source--list">
        {buckets.map(bucket => (
          <SelectorListItem
            key={bucket.name}
            value={bucket}
            onClick={updateBucket}
            selected={bucket.name === bucketName}
            text={bucket.name}
          />
        ))}
      </DapperScrollbars>
    )
  }

  return (
    <div className="data-source--block">
      <div className="data-source--block-title">Bucket</div>
      {body}
    </div>
  )
}

export default BucketSelector
