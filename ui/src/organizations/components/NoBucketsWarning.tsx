// Libraries
import React, {SFC} from 'react'

interface Props {
  visible: boolean
  resourceName: string
}

const NoBucketsWarning: SFC<Props> = ({visible, resourceName}) => {
  return (
    visible && (
      <div>
        You don't currently have any buckets. Any{' '}
        <strong>{resourceName}</strong> you have will not be able to write data
        until a bucket is created and they are directed to that bucket.
      </div>
    )
  )
}

export default NoBucketsWarning
