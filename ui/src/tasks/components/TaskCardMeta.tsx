// Libraries
import React, {FC} from 'react'
import CopyToClipboard from 'react-copy-to-clipboard'
import {connect, ConnectedProps} from 'react-redux'

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
import {Task} from 'src/types'

interface OwnProps {
  task: Task
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

const TaskCardMeta: FC<Props> = ({task, notify}) => {
  const handleCopyAttempt = (
    copiedText: string,
    isSuccessful: boolean
  ): void => {
    const text = copiedText.slice(0, 30).trimRight()
    const truncatedText = `${text}...`

    if (isSuccessful) {
      notify(copyToClipboardSuccess(truncatedText, 'Task ID'))
    } else {
      notify(copyToClipboardFailed(truncatedText, 'Task ID'))
    }
  }

  return (
    <ResourceCard.Meta>
      <CopyToClipboard text={task.id} onCopy={handleCopyAttempt}>
        <span className="copy-task-id" title="Click to Copy to Clipboard">
          ID: {task.id}
          <span className="copy-task-id--helper">Copy to Clipboard</span>
        </span>
      </CopyToClipboard>
    </ResourceCard.Meta>
  )
}

const mdtp = {
  notify: notifyAction,
}

const connector = connect(null, mdtp)

export default connector(TaskCardMeta)
