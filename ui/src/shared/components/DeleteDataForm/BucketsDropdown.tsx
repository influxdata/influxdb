// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {SelectDropdown} from '@influxdata/clockface'

// Types
import {AppState} from 'src/types'

// Selectors
import {getSortedBuckets} from 'src/buckets/selectors'

interface StateProps {
  bucketNames: string[]
}

interface OwnProps {
  bucketName: string
  onSetBucketName: (bucketName: string) => any
}

type Props = StateProps & OwnProps

const BucketsDropdown: FunctionComponent<Props> = ({
  bucketNames,
  bucketName,
  onSetBucketName,
}) => {
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
  const buckets = getSortedBuckets(state)

  return {
    bucketNames: buckets.map(bucket => bucket.name),
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(BucketsDropdown)
