// Libraries
import React, {useMemo, useState, FC} from 'react'
import {Page} from '@influxdata/clockface'

// Components
import EventViewer from 'src/eventViewer/components/EventViewer'
import EventTable from 'src/eventViewer/components/EventTable'
import AlertHistoryControls from 'src/alerting/containers/AlertHistoryControls'

// Constants
import {
  STATUS_FIELDS,
  NOTIFICATION_FIELDS,
} from 'src/alerting/constants/history'

// Utils
import {loadStatuses, loadNotifications} from 'src/alerting/utils/history'

// Types
import {AlertHistoryType} from 'src/types'

interface Props {
  params: {orgID: string}
}

const AlertHistoryIndex: FC<Props> = ({params: {orgID}}) => {
  const [historyType, setHistoryType] = useState<AlertHistoryType>('statuses')

  const loadRows = useMemo(() => {
    return historyType === 'statuses'
      ? options => loadStatuses(orgID, options)
      : options => loadNotifications(orgID, options)
  }, [orgID, historyType])

  const fields =
    historyType === 'statuses' ? STATUS_FIELDS : NOTIFICATION_FIELDS

  return (
    <EventViewer loadRows={loadRows}>
      {props => (
        <Page
          titleTag="Check Statuses | InfluxDB 2.0"
          className="alert-history-page"
        >
          <Page.Header fullWidth={true}>
            <div className="alert-history-page--header">
              <Page.Title title="Check Statuses" />
              <AlertHistoryControls
                historyType={historyType}
                onSetHistoryType={setHistoryType}
                eventViewerProps={props}
              />
            </div>
          </Page.Header>
          <Page.Contents
            fullWidth={true}
            fullHeight={true}
            scrollable={false}
            className="alert-history-page--contents"
          >
            <div className="alert-history">
              <EventTable {...props} fields={fields} />
            </div>
          </Page.Contents>
        </Page>
      )}
    </EventViewer>
  )
}

export default AlertHistoryIndex
