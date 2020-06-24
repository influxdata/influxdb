// Libraries
import React, {FC, useEffect} from 'react'
import {connect} from 'react-redux'

// Components
import {DapperScrollbars} from '@influxdata/clockface'
import SelectorListItem from 'src/notebooks/pipes/Data/SelectorListItem'

// Types
import {PipeData} from 'src/notebooks'
import {AppState, Bucket, ResourceType} from 'src/types'

// Utils
import {getAll} from 'src/resources/selectors'

interface OwnProps {
  onUpdate: (data: any) => void
  data: PipeData
}

interface StateProps {
  buckets: Bucket[]
}

type Props = OwnProps & StateProps

const BucketSelector: FC<Props> = ({onUpdate, data, buckets}) => {
  const bucketName = data.bucketName

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

const mstp = (state: AppState) => {
  const buckets = getAll<Bucket>(state, ResourceType.Buckets)

  return {buckets}
}

export default connect<StateProps, {}, OwnProps>(mstp, null)(BucketSelector)
