// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Types
import {NotificationRuleDraft, AppState} from 'src/types'

// Components
import {
  Button,
  IconFont,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'
import NotificationRuleCards from 'src/alerting/components/notifications/RuleCards'
import AlertsColumn from 'src/alerting/components/AlertsColumn'

interface StateProps {
  rules: NotificationRuleDraft[]
  endpoints: AppState['endpoints']['list']
}

type Props = StateProps & WithRouterProps

const NotificationRulesColumn: FunctionComponent<Props> = ({
  rules,
  router,
  params,
  endpoints,
}) => {
  const handleOpenOverlay = () => {
    const newRuleRoute = `/orgs/${params.orgID}/alerting/rules/new`
    router.push(newRuleRoute)
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
      title="Notification Rules"
      createButton={createButton}
      questionMarkTooltipContents={tooltipContents}
    >
      <NotificationRuleCards rules={rules} />
    </AlertsColumn>
  )
}

const mstp = (state: AppState) => {
  const {
    rules: {list: rules},
    endpoints,
  } = state

  return {rules, endpoints: endpoints.list}
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(withRouter(NotificationRulesColumn))
