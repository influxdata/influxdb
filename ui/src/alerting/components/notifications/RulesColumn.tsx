// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

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
import OverlayLink from 'src/overlays/components/OverlayLink'

interface StateProps {
  rules: NotificationRuleDraft[]
  endpoints: AppState['endpoints']['list']
}

type Props = StateProps

const NotificationRulesColumn: FunctionComponent<Props> = ({
  rules,
  endpoints,
}) => {
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
    <OverlayLink overlayID="create-notification-rule">
      {onClick => (
        <Button
          color={ComponentColor.Secondary}
          text="Create"
          onClick={onClick}
          testID="create-rule"
          icon={IconFont.Plus}
          status={buttonStatus}
          titleText={buttonTitleText}
        />
      )}
    </OverlayLink>
  )

  return (
    <AlertsColumn
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
  const {
    rules: {list: rules},
    endpoints,
  } = state

  return {rules, endpoints: endpoints.list}
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(NotificationRulesColumn)
