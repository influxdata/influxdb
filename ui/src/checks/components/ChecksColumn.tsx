// Libraries
import React, {FunctionComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router-dom'
import {connect} from 'react-redux'

// Selectors
import {getAll} from 'src/resources/selectors'
import {sortChecksByName} from 'src/checks/selectors'
import {sortRulesByName} from 'src/notifications/rules/selectors'
import {sortEndpointsByName} from 'src/notifications/endpoints/selectors'

// Components
import CheckCards from 'src/checks/components/CheckCards'
import AlertsColumn from 'src/alerting/components/AlertsColumn'
import CreateCheckDropdown from 'src/checks/components/CreateCheckDropdown'

// Types
import {
  Check,
  NotificationRuleDraft,
  AppState,
  NotificationEndpoint,
  ResourceType,
} from 'src/types'

interface StateProps {
  checks: Check[]
  rules: NotificationRuleDraft[]
  endpoints: NotificationEndpoint[]
}

type Props = StateProps & WithRouterProps

const ChecksColumn: FunctionComponent<Props> = ({
  checks,
  router,
  params: {orgID},
  rules,
  endpoints,
}) => {
  const handleCreateThreshold = () => {
    router.push(`/orgs/${orgID}/alerting/checks/new-threshold`)
  }

  const handleCreateDeadman = () => {
    router.push(`/orgs/${orgID}/alerting/checks/new-deadman`)
  }

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

  const createButton = (
    <CreateCheckDropdown
      onCreateThreshold={handleCreateThreshold}
      onCreateDeadman={handleCreateDeadman}
    />
  )

  return (
    <AlertsColumn
      type={ResourceType.Checks}
      title="Checks"
      createButton={createButton}
      questionMarkTooltipContents={tooltipContents}
    >
      {searchTerm => (
        <CheckCards
          checks={checks}
          searchTerm={searchTerm}
          onCreateThreshold={handleCreateThreshold}
          onCreateDeadman={handleCreateDeadman}
          showFirstTimeWidget={noAlertingResourcesExist}
        />
      )}
    </AlertsColumn>
  )
}

const mstp = (state: AppState) => {
  const checks = getAll<Check>(state, ResourceType.Checks)

  const endpoints = getAll<NotificationEndpoint>(
    state,
    ResourceType.NotificationEndpoints
  )

  const rules = getAll<NotificationRuleDraft>(
    state,
    ResourceType.NotificationRules
  )

  return {
    checks: sortChecksByName(checks),
    rules: sortRulesByName(rules),
    endpoints: sortEndpointsByName(endpoints),
  }
}

export default connect<StateProps>(mstp, null)(withRouter(ChecksColumn))
