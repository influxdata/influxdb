// Libraries
import React, {FC} from 'react'
import {SelectGroup} from '@influxdata/clockface'

// Components
import BackToTopButton from 'src/eventViewer/components/BackToTopButton'
import SearchBar from 'src/alerting/components/SearchBar'
import {Page} from '@influxdata/clockface'

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
    <>
      <Page.ControlBarLeft>
        <SelectGroup className="alert-history-controls--switcher">
          <SelectGroup.Option
            name="alert-history-mode"
            id="alert-history-mode--statuses"
            value="statuses"
            onClick={() => onSetHistoryType('statuses')}
            titleText="View Status History"
            active={historyType === 'statuses'}
          >
            Statuses
          </SelectGroup.Option>
          <SelectGroup.Option
            name="alert-history-mode"
            id="alert-history-mode--notifications"
            value="notifications"
            onClick={() => onSetHistoryType('notifications')}
            titleText="View Notification History"
            active={historyType === 'notifications'}
          >
            Notifications
          </SelectGroup.Option>
        </SelectGroup>
      </Page.ControlBarLeft>
      <Page.ControlBarRight>
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
      </Page.ControlBarRight>
    </>
  )
}

export default AlertHistoryControls
