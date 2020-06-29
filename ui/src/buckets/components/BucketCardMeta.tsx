// Libraries
import React, {FC} from 'react'
import CopyToClipboard from 'react-copy-to-clipboard'
import {capitalize} from 'lodash'
import {connect} from 'react-redux'

// Constants
import {
  copyToClipboardSuccess,
  copyToClipboardFailed,
} from 'src/shared/copy/notifications'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Components
import {ResourceCard} from '@influxdata/clockface'

// Types
import {OwnBucket} from 'src/types'

interface DispatchProps {
  notify: typeof notifyAction
}

interface OwnProps {
  bucket: OwnBucket
}

type Props = OwnProps & DispatchProps

const BucketCardMeta: FC<Props> = ({bucket, notify}) => {
  const handleCopyAttempt = (
    copiedText: string,
    isSuccessful: boolean
  ): void => {
    const text = copiedText.slice(0, 30).trimRight()
    const truncatedText = `${text}...`

    if (isSuccessful) {
      notify(copyToClipboardSuccess(truncatedText, 'Bucket ID'))
    } else {
      notify(copyToClipboardFailed(truncatedText, 'Bucket ID'))
    }
  }

  const persistentBucketMeta = (
    <span data-testid="bucket-retention">
      Retention: {capitalize(bucket.readableRetention)}
    </span>
  )

  if (bucket.type === 'system') {
    return (
      <ResourceCard.Meta>
        <span
          className="system-bucket"
          key={`system-bucket-indicator-${bucket.id}`}
        >
          System Bucket
        </span>
        {persistentBucketMeta}
      </ResourceCard.Meta>
    )
  }

  return (
    <ResourceCard.Meta>
      {persistentBucketMeta}
      <CopyToClipboard text={bucket.id} onCopy={handleCopyAttempt}>
        <span className="copy-bucket-id" title="Click to Copy to Clipboard">
          ID: {bucket.id}
          <span className="copy-bucket-id--helper">Copy to Clipboard</span>
        </span>
      </CopyToClipboard>
    </ResourceCard.Meta>
  )
}

const mdtp: DispatchProps = {
  notify: notifyAction,
}

export default connect<{}, DispatchProps, OwnProps>(null, mdtp)(BucketCardMeta)
