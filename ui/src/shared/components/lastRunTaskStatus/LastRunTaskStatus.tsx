import React, {FC} from 'react'
import {Icon, IconFont} from '@influxdata/clockface'

interface PassedProps {
  lastRunError?: string
  lastRunStatus: string
}

const LastRunTaskStatus: FC<PassedProps> = ({lastRunError, lastRunStatus}) => {
  if (lastRunStatus === 'failed' || lastRunError !== undefined) {
    return (
      <li className="query-checklist--item error">
        <Icon glyph={IconFont.Remove} />
        {lastRunError}
      </li>
    )
  }
  if (lastRunStatus === 'cancel') {
    return (
      <li className="query-checklist--item error">
        <Icon glyph={IconFont.Remove} />
        Task Cancelled
      </li>
    )
  }
  return (
    <li className="query-checklist--item valid">
      <Icon glyph={IconFont.Checkmark} />
      Task ran successfully!
    </li>
  )
}

export default LastRunTaskStatus
