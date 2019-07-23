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
  const bucketNames = buckets.map(bucket => bucket.name)
  return (
    <SelectDropdown
      options={bucketNames}
      selectedOption={bucketName}
      onSelect={onSetBucketName}
    />
  )
}

const mstp = (state: AppState): StateProps => ({
  buckets: state.buckets.list,
})

export default connect<StateProps, {}, OwnProps>(mstp)(BucketsDropdown)
