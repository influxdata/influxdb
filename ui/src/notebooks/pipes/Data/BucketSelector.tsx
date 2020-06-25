// Libraries
import React, {FC, useEffect, useContext} from 'react'

// Components
import {DapperScrollbars} from '@influxdata/clockface'
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
    if (updatedBucket) {
      const bucketName = updatedBucket.name
      onUpdate({bucketName})
    }
  }

  useEffect(() => {
    updateBucket(buckets[0])
  }, [])

  if (bucketName) {
    return (
      <div className="data-source--block">
        <div className="data-source--block-title">Bucket</div>
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
      </div>
    )
  }

  return <p>No Buckets</p>
}

export default BucketSelector
