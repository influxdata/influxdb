// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {SelectDropdown} from '@influxdata/clockface'

// Types
import {AppState} from 'src/types'

// Selectors
import {sortBucketNames} from 'src/buckets/selectors/index'

interface StateProps {
  buckets: string[]
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
  return (
    <SelectDropdown
      options={buckets}
      selectedOption={bucketName}
      onSelect={onSetBucketName}
    />
  )
}

const mstp = (state: AppState): StateProps => {
  // map names and sort via a selector
  const buckets = sortBucketNames(state.buckets.list)
  return {
    buckets,
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(BucketsDropdown)
