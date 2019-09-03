// Libraries
import React, {FC} from 'react'
import {Radio} from '@influxdata/clockface'

// Components
import BackToTopButton from 'src/eventViewer/components/BackToTopButton'
import SearchBar from 'src/alerting/components/SearchBar'

// Types
import {AlertHistoryType} from 'src/types'
import {EventViewerChildProps} from 'src/eventViewer/types'

// Constants
import {
  EXAMPLE_STATUS_SEARCHES,
  EXAMPLE_NOTIFICATION_SEARCHES,
} from 'src/alerting/constants/history'

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
        <SearchBar
          {...eventViewerProps}
          placeholder={`Search ${historyType}...`}
          exampleSearches={
            historyType === 'statuses'
              ? EXAMPLE_STATUS_SEARCHES
              : EXAMPLE_NOTIFICATION_SEARCHES
          }
        />
      </div>
    </div>
  )
}

export default AlertHistoryControls
