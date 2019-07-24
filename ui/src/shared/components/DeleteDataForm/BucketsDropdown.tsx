// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {Dropdown} from '@influxdata/clockface'

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
  return (
    <Dropdown selectedID={bucketName} onChange={onSetBucketName}>
      {buckets.map(bucket => (
        <Dropdown.Item key={bucket.name} id={bucket.name} value={bucket.name}>
          {bucket.name}
        </Dropdown.Item>
      ))}
    </Dropdown>
  )
}

const mstp = (state: AppState): StateProps => ({
  buckets: state.buckets.list,
})

export default connect<StateProps, {}, OwnProps>(mstp)(BucketsDropdown)
