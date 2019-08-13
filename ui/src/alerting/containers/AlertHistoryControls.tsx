// Libraries
import React, {FC} from 'react'
import {Radio} from '@influxdata/clockface'

// Components
import BackToTopButton from 'src/eventViewer/components/BackToTopButton'
import LimitDropdown from 'src/eventViewer/components/LimitDropdown'
import StatusSearchBar from 'src/alerting/components/StatusSearchBar'

// Types
import {AlertHistoryType} from 'src/types'
import {EventViewerChildProps} from 'src/eventViewer/types'

interface Props {
  historyType: AlertHistoryType
  onSetHistoryType: (t: AlertHistoryType) => void
  eventViewerProps: EventViewerChildProps
}

const AlertHistoryControls: FC<Props> = ({
  eventViewerProps,
  historyType,
  onSetHistoryType,
}) => {
  return (
    <div className="alert-history-controls">
      <div className="alert-history-controls--left">
        <Radio className="alert-history-controls--switcher">
          <Radio.Button
            value="statuses"
            onClick={() => onSetHistoryType('statuses')}
            titleText="View Status History"
            active={historyType === 'statuses'}
          >
            Statuses
          </Radio.Button>
          <Radio.Button
            value="notifications"
            onClick={() => onSetHistoryType('notifications')}
            titleText="View Notification History"
            active={historyType === 'notifications'}
          >
            Notifications
          </Radio.Button>
        </Radio>
      </div>
      <div className="alert-history-controls--right">
        <BackToTopButton {...eventViewerProps} />
        <LimitDropdown {...eventViewerProps} />
        <StatusSearchBar {...eventViewerProps} />
      </div>
    </div>
  )
}

export default AlertHistoryControls
