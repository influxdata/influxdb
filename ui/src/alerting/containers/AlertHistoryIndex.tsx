// Libraries
import React, {FC} from 'react'
import {Page} from '@influxdata/clockface'

// Components
import EventViewer from 'src/eventViewer/components/EventViewer'
import EventTable from 'src/eventViewer/components/EventTable'
import BackToTopButton from 'src/eventViewer/components/BackToTopButton'
import LimitDropdown from 'src/eventViewer/components/LimitDropdown'
import StatusSearchBar from 'src/alerting/components/StatusSearchBar'
import StatusTableField from 'src/alerting/components/StatusTableField'
import TagsTableField from 'src/alerting/components/TagsTableField'
import TimeTableField from 'src/alerting/components/TimeTableField'
import CheckNameTableField from 'src/alerting/components/CheckNameTableField'

// Utils
import {fakeLoadRows} from 'src/eventViewer/utils/fakeLoadRows'

// Types
import {FieldComponents} from 'src/eventViewer/types'

const FIELD_COMPONENTS: FieldComponents = {
  time: TimeTableField,
  status: StatusTableField,
  tags: TagsTableField,
  checkName: CheckNameTableField,
}

const FIELD_WIDTHS = {
  time: 160,
  checkName: 100,
  message: 300,
  status: 50,
  tags: 300,
}

const AlertHistoryIndex: FC = () => {
  return (
    <EventViewer loadRows={fakeLoadRows}>
      {props => (
        <Page
          titleTag="Alert History | InfluxDB 2.0"
          className="alert-history-page"
        >
          <Page.Header fullWidth={true}>
            <div className="alert-history-page--header">
              <Page.Title title="Alert History" />
              <div className="alert-history-page--controls">
                <BackToTopButton {...props} />
                <LimitDropdown {...props} />
                <StatusSearchBar {...props} />
              </div>
            </div>
          </Page.Header>
          <Page.Contents
            fullWidth={true}
            fullHeight={true}
            scrollable={false}
            className="alert-history-page--contents"
          >
            <div className="alert-history">
              <EventTable
                {...props}
                fields={['time', 'checkName', 'status', 'message', 'tags']}
                fieldWidths={FIELD_WIDTHS}
                fieldComponents={FIELD_COMPONENTS}
              />
            </div>
          </Page.Contents>
        </Page>
      )}
    </EventViewer>
  )
}

export default AlertHistoryIndex
