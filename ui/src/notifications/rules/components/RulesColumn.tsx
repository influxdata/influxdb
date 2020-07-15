// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Types
import {
  NotificationEndpoint,
  NotificationRuleDraft,
  AppState,
  ResourceType,
} from 'src/types'

// Components
import {
  Button,
  IconFont,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'
import NotificationRuleCards from 'src/notifications/rules/components/RuleCards'
import AlertsColumn from 'src/alerting/components/AlertsColumn'

// Selectors
import {getAll} from 'src/resources/selectors'

interface StateProps {
  rules: NotificationRuleDraft[]
  endpoints: NotificationEndpoint[]
}

type Props = StateProps & RouteComponentProps<{orgID: string}>

const NotificationRulesColumn: FunctionComponent<Props> = ({
  rules,
  history,
  match,
  endpoints,
}) => {
  const handleOpenOverlay = () => {
    const newRuleRoute = `/orgs/${match.params.orgID}/alerting/rules/new`
    history.push(newRuleRoute)
  }

  const tooltipContents = (
    <>
      A <strong>Notification Rule</strong> will query statuses
      <br />
      written by <strong>Checks</strong> to determine if a
      <br />
      notification should be sent to a
      <br />
      <strong>Notification Endpoint</strong>
      <br />
      <br />
      <a
        href="https://v2.docs.influxdata.com/v2.0/monitor-alert/notification-rules/create"
        target="_blank"
      >
        Read Documentation
      </a>
    </>
  )

  const buttonStatus = !!endpoints.length
    ? ComponentStatus.Default
    : ComponentStatus.Disabled

  const buttonTitleText = !!endpoints.length
    ? 'Create a Notification Rule'
    : 'You need at least 1 Notifcation Endpoint to create a Notification Rule'

  const createButton = (
    <Button
      color={ComponentColor.Secondary}
      text="Create"
      onClick={handleOpenOverlay}
      testID="create-rule"
      icon={IconFont.Plus}
      status={buttonStatus}
      titleText={buttonTitleText}
    />
  )

  return (
    <AlertsColumn
      type={ResourceType.NotificationRules}
      title="Notification Rules"
      createButton={createButton}
      questionMarkTooltipContents={tooltipContents}
    >
      {searchTerm => (
        <NotificationRuleCards rules={rules} searchTerm={searchTerm} />
      )}
    </AlertsColumn>
  )
}

const mstp = (state: AppState) => {
  const rules = getAll<NotificationRuleDraft>(
    state,
    ResourceType.NotificationRules
  )

  const endpoints = getAll<NotificationEndpoint>(
    state,
    ResourceType.NotificationEndpoints
  )

  return {rules, endpoints}
}

export default connect<StateProps>(
  mstp,
  null
)(withRouter(NotificationRulesColumn))
