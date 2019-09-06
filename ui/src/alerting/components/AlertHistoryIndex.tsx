// Libraries
import React, {useMemo, useState, FC} from 'react'
import {Page} from '@influxdata/clockface'

// Components
import EventViewer from 'src/eventViewer/components/EventViewer'
import EventTable from 'src/eventViewer/components/EventTable'
import AlertHistoryControls from 'src/alerting/components/AlertHistoryControls'
import AlertHistoryQueryParams from 'src/alerting/components/AlertHistoryQueryParams'

// Constants
import {
  STATUS_FIELDS,
  NOTIFICATION_FIELDS,
} from 'src/alerting/constants/history'

// Utils
import {
  loadStatuses,
  loadNotifications,
  getInitialHistoryType,
  getInitialState,
} from 'src/alerting/utils/history'

// Types
import {AlertHistoryType} from 'src/types'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

interface Props {
  params: {orgID: string}
}

const AlertHistoryIndex: FC<Props> = ({params: {orgID}}) => {
  const [historyType, setHistoryType] = useState<AlertHistoryType>(
    getInitialHistoryType()
  )

  const loadRows = useMemo(() => {
    return historyType === 'statuses'
      ? options => loadStatuses(orgID, options)
      : options => loadNotifications(orgID, options)
  }, [orgID, historyType])

  const fields =
    historyType === 'statuses' ? STATUS_FIELDS : NOTIFICATION_FIELDS

  return (
    <GetResources resource={ResourceTypes.Checks}>
      <GetResources resource={ResourceTypes.NotificationEndpoints}>
        <GetResources resource={ResourceTypes.NotificationRules}>
          <EventViewer loadRows={loadRows} initialState={getInitialState()}>
            {props => (
              <Page
                titleTag="Check Statuses | InfluxDB 2.0"
                className="alert-history-page"
              >
                <Page.Header fullWidth={true}>
                  <div className="alert-history-page--header">
                    <Page.Title title="Check Statuses" />
                    <AlertHistoryQueryParams
                      searchInput={props.state.searchInput}
                      historyType={historyType}
                    />
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
        </GetResources>
      </GetResources>
    </GetResources>
  )
}

export default AlertHistoryIndex
