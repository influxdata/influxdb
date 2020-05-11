// Libraries
import React, {useMemo, FC} from 'react'
import {connect} from 'react-redux'

// Components
import {Page} from '@influxdata/clockface'
import EventViewer from 'src/eventViewer/components/EventViewer'
import CheckHistoryControls from 'src/checks/components/CheckHistoryControls'
import CheckHistoryVisualization from 'src/checks/components/CheckHistoryVisualization'
import AlertHistoryQueryParams from 'src/alerting/components/AlertHistoryQueryParams'
import EventTable from 'src/eventViewer/components/EventTable'
import GetResources from 'src/resources/components/GetResources'
import CloudUpgradeButton from 'src/shared/components/CloudUpgradeButton'

//Context
import {ResourceIDsContext} from 'src/alerting/components/AlertHistoryIndex'

// Constants
import {STATUS_FIELDS} from 'src/alerting/constants/history'

// Utils
import {loadStatuses, getInitialState} from 'src/alerting/utils/history'
import {getCheckIDs} from 'src/checks/selectors'
import {getTimeZone} from 'src/dashboards/selectors'

// Types
import {ResourceIDs} from 'src/checks/reducers'
import {AppState, Check, TimeZone, ResourceType} from 'src/types'
import {getByID} from 'src/resources/selectors'

interface OwnProps {
  params: {orgID: string; checkID: string}
}

interface StateProps {
  check: Check
  timeZone: TimeZone
  resourceIDs: ResourceIDs
}

type Props = OwnProps & StateProps

const CheckHistory: FC<Props> = ({
  params: {orgID},
  check,
  timeZone,
  resourceIDs,
}) => {
  const loadRows = useMemo(() => options => loadStatuses(orgID, options), [
    orgID,
  ])
  const historyType = 'statuses'
  const fields = STATUS_FIELDS
  return (
    <GetResources resources={[ResourceType.Checks]}>
      <ResourceIDsContext.Provider value={resourceIDs}>
        <EventViewer loadRows={loadRows} initialState={getInitialState()}>
          {props => (
            <Page
              titleTag="Check Statuses | InfluxDB 2.0"
              className="alert-history-page"
            >
              <Page.Header fullWidth={true}>
                <Page.Title
                  title="Check Statuses"
                  testID="alert-history-title"
                />
                <CloudUpgradeButton />
              </Page.Header>
              <Page.ControlBar fullWidth={true}>
                <Page.ControlBarLeft>
                  <CheckHistoryControls eventViewerProps={props} />
                  <AlertHistoryQueryParams
                    searchInput={props.state.searchInput}
                    historyType={historyType}
                  />
                </Page.ControlBarLeft>
              </Page.ControlBar>
              <Page.Contents
                fullWidth={true}
                scrollable={false}
                className="alert-history-page--contents"
              >
                <div className="alert-history-contents">
                  {check.type !== 'custom' && (
                    <CheckHistoryVisualization
                      check={check}
                      timeZone={timeZone}
                    />
                  )}
                  <div className="alert-history">
                    <EventTable {...props} fields={fields} />
                  </div>
                </div>
              </Page.Contents>
            </Page>
          )}
        </EventViewer>
      </ResourceIDsContext.Provider>
    </GetResources>
  )
}

const mstp = (state: AppState, props: OwnProps) => {
  const timeZone = getTimeZone(state)
  const checkIDs = getCheckIDs(state)
  const check = getByID<Check>(state, ResourceType.Checks, props.params.checkID)

  const resourceIDs = {
    checkIDs,
    endpointIDs: null,
    ruleIDs: null,
  }

  return {check, timeZone, resourceIDs}
}

export default connect<StateProps>(mstp)(CheckHistory)
