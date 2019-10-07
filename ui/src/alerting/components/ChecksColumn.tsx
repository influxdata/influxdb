// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Selectors
import {viewableLabels} from 'src/labels/selectors'

// Components
import CheckCards from 'src/alerting/components/CheckCards'
import AlertsColumn from 'src/alerting/components/AlertsColumn'
import CreateCheckDropdown from 'src/alerting/components/CreateCheckDropdown'

// Types
import {Check, NotificationRuleDraft, AppState} from 'src/types'

interface StateProps {
  checks: Check[]
  rules: NotificationRuleDraft[]
  endpoints: AppState['endpoints']['list']
}

const ChecksColumn: FunctionComponent<StateProps> = ({
  checks,
  rules,
  endpoints,
}) => {
  const tooltipContents = (
    <>
      A <strong>Check</strong> is a periodic query that the system
      <br />
      performs against your time series data
      <br />
      that will generate a status
      <br />
      <br />
      <a
        href="https://v2.docs.influxdata.com/v2.0/monitor-alert/checks/create/"
        target="_blank"
      >
        Read Documentation
      </a>
    </>
  )

  const noAlertingResourcesExist =
    !checks.length && !rules.length && !endpoints.length

  const createButton = <CreateCheckDropdown />

  return (
    <AlertsColumn
      title="Checks"
      createButton={createButton}
      questionMarkTooltipContents={tooltipContents}
    >
      {searchTerm => (
        <CheckCards
          checks={checks}
          searchTerm={searchTerm}
          showFirstTimeWidget={noAlertingResourcesExist}
        />
      )}
    </AlertsColumn>
  )
}

const mstp = (state: AppState) => {
  const {
    checks: {list: checks},
    labels: {list: labels},
    rules: {list: rules},
    endpoints,
  } = state

  return {
    checks,
    labels: viewableLabels(labels),
    rules,
    endpoints: endpoints.list,
  }
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(ChecksColumn)
