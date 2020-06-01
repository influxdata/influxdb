// Libraries
import React, {FC} from 'react'
import {capitalize} from 'lodash'

// Components
import {ResourceCard} from '@influxdata/clockface'

// Types
import {OwnBucket} from 'src/types'

interface Props {
  bucket: OwnBucket
}

const BucketCardMeta: FC<Props> = ({bucket}) => {
  let systemTypeIndicator

  if (bucket.type !== 'user') {
    systemTypeIndicator = (
      <span
        className="system-bucket"
        key={`system-bucket-indicator-${bucket.id}`}
      >
        System Bucket
      </span>
    )
  }

  return (
    <ResourceCard.Meta>
      {systemTypeIndicator}
      <span data-testid="bucket-retention">
        Retention: {capitalize(bucket.readableRetention)}
      </span>
      <span>ID: {bucket.id}</span>
    </ResourceCard.Meta>
  )
}

export default BucketCardMeta
