// Libraries
import React, {useMemo, FC, createContext, useState, useEffect} from 'react'
import {Page} from '@influxdata/clockface'
import {Plot} from '@influxdata/giraffe'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import EventViewer from 'src/eventViewer/components/EventViewer'
import EventTable from 'src/eventViewer/components/EventTable'
import CheckHistoryControls from 'src/alerting/components/CheckHistoryControls'
import AlertHistoryQueryParams from 'src/alerting/components/AlertHistoryQueryParams'
import CheckPlot from 'src/shared/components/CheckPlot'
import EmptyQueryView, {ErrorFormat} from 'src/shared/components/EmptyQueryView'

// Constants
import {STATUS_FIELDS} from 'src/alerting/constants/history'

// Utils
import {loadStatuses, getInitialState} from 'src/alerting/utils/history'

// Types
import GetResources, {ResourceType} from 'src/shared/components/GetResources'
import {AppState, Check, TimeZone, CheckViewProperties} from 'src/types'
import TimeSeries from 'src/shared/components/TimeSeries'
import {createView} from 'src/shared/utils/view'
import {checkResultsLength} from 'src/shared/utils/vis'
import {getTimeRangeVars} from 'src/variables/utils/getTimeRangeVars'

interface ResourceIDs {
  checkIDs: {[x: string]: boolean}
  endpointIDs: {[x: string]: boolean}
  ruleIDs: {[x: string]: boolean}
}

export const ResourceIDsContext = createContext<ResourceIDs>(null)

interface OwnProps {
  params: {orgID: string; checkID: string}
}

interface StateProps {
  check: Check
  timeZone: TimeZone
}

type Props = OwnProps & StateProps
//TODO maybe update submitToken when we know how

const CheckHistory: FC<Props> = ({
  params: {orgID, checkID},
  check,
  timeZone,
}) => {
  const historyType = 'statuses'
  let properties: CheckViewProperties

  useEffect(() => {
    const view = createView<CheckViewProperties>(
      get(check, 'type', 'threshold')
    )
    properties = view.properties
  }, [check])

  const loadRows = useMemo(() => options => loadStatuses(orgID, options), [
    orgID,
  ])
  const [submitToken, setSubmitToken] = useState(0)
  const [manualRefresh, setManualRefresh] = useState(0)

  const fields = STATUS_FIELDS
  return (
    <GetResources resources={[ResourceType.Checks]}>
      <EventViewer loadRows={loadRows} initialState={getInitialState()}>
        {props => (
          <Page
            titleTag="Check Statuses | InfluxDB 2.0"
            className="alert-history-page"
          >
            <Page.Header fullWidth={true}>
              <div className="alert-history-page--header">
                <Page.Title
                  title="Check Statuses"
                  testID="alert-history-title"
                />
                <AlertHistoryQueryParams
                  searchInput={props.state.searchInput}
                  historyType={historyType}
                />
                <CheckHistoryControls eventViewerProps={props} />
              </div>
            </Page.Header>
            <Page.Contents
              fullWidth={true}
              scrollable={false}
              className="alert-history-page--contents"
            >
              {/* {<CheckHistoryVisualization .../>} */}
              <TimeSeries
                submitToken={submitToken}
                queries={[check.query]}
                key={manualRefresh}
                variables={getTimeRangeVars({lower: 'now() - 5m'})}
                check={check}
              >
                {({
                  giraffeResult,
                  loading,
                  errorMessage,
                  isInitialFetch,
                  statuses,
                }) => {
                  return (
                    <EmptyQueryView
                      errorFormat={ErrorFormat.Tooltip}
                      errorMessage={errorMessage}
                      hasResults={checkResultsLength(giraffeResult)}
                      loading={loading}
                      isInitialFetch={isInitialFetch}
                      queries={[check.query]}
                      fallbackNote={null}
                    >
                      <CheckPlot
                        check={check}
                        table={get(giraffeResult, 'table')}
                        fluxGroupKeyUnion={get(
                          giraffeResult,
                          'fluxGroupKeyUnion'
                        )}
                        loading={loading}
                        timeZone={timeZone}
                        viewProperties={properties}
                        statuses={statuses}
                      >
                        {config => <Plot config={config} />}
                      </CheckPlot>
                      {/* {<CheckHistoryStatuses .../>} */}
                    </EmptyQueryView>
                  )
                }}
              </TimeSeries>
              <div className="alert-history">
                <EventTable {...props} fields={fields} />
              </div>
            </Page.Contents>
          </Page>
        )}
      </EventViewer>
    </GetResources>
  )
}

const mstp = (state: AppState, props: OwnProps) => {
  const check = state.checks.list.find(({id}) => id === props.params.checkID)
  const timeZone = state.app.persisted.timeZone

  return {check, timeZone}
}

export default connect<StateProps>(mstp)(CheckHistory)
