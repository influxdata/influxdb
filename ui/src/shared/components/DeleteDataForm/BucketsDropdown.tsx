// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {SelectDropdown} from '@influxdata/clockface'

// Types
import {AppState, Bucket} from 'src/types'

interface StateProps {
  buckets: Bucket[]
}

interface OwnProps {
  bucketName: string
  onSetBucketName: (bucketName: string) => any
}

type Props = StateProps & OwnProps

const BucketsDropdown: FunctionComponent<Props> = ({
  buckets,
  bucketName,
  onSetBucketName,
}) => {
  const bucketNames = buckets
    .map(bucket => bucket.name)
    .sort((a, b) => {
      if (isDefaultBucket(a)) {
        // ensures that the default _monitoring && _tasks are the last buckets
        return 1
      }
      if (`${a}`.toLowerCase() < `${b}`.toLowerCase()) {
        return -1
      }
      if (`${a}`.toLowerCase() > `${b}`.toLowerCase()) {
        return 1
      }
      return 0
    })
  return (
    <SelectDropdown
      options={bucketNames}
      selectedOption={bucketName}
      onSelect={onSetBucketName}
    />
  )
}

const mstp = (state: AppState): StateProps => {
  // map names and sort via a selector
  return {
    buckets: state.buckets.list,
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(BucketsDropdown)
