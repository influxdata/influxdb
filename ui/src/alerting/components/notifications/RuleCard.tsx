// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {SlideToggle, ComponentSize, ResourceCard} from '@influxdata/clockface'
import NotificationRuleCardContext from 'src/alerting/components/notifications/RuleCardContext'

// Constants
import {DEFAULT_NOTIFICATION_RULE_NAME} from 'src/alerting/constants'

// Action
import {updateRule, deleteRule} from 'src/alerting/actions/notifications/rules'

// Types
import {NotificationRuleDraft} from 'src/types'

interface DispatchProps {
  updateRule: typeof updateRule
  deleteNotificationRule: typeof deleteRule
}

interface OwnProps {
  rule: NotificationRuleDraft
}

type Props = OwnProps & DispatchProps & WithRouterProps

const RuleCard: FunctionComponent<Props> = ({
  rule,
  updateRule,
  deleteNotificationRule,
  params: {orgID},
  router,
}) => {
  const onUpdateName = (name: string) => {
    updateRule({...rule, name})
  }

  const onDelete = () => {
    deleteNotificationRule(rule.id)
  }

  const onExport = () => {}

  const onClone = () => {}

  const onToggle = () => {
    const status = rule.status === 'active' ? 'inactive' : 'active'

    updateRule({...rule, status})
  }

  const onRuleClick = () => {
    router.push(`/orgs/${orgID}/alerting/rules/${rule.id}/edit`)
  }

  return (
    <ResourceCard
      key={`rule-id--${rule.id}`}
      testID="rule-card"
      name={
        <ResourceCard.EditableName
          onUpdate={onUpdateName}
          onClick={onRuleClick}
          name={rule.name}
          noNameString={DEFAULT_NOTIFICATION_RULE_NAME}
          testID="rule-card--name"
          buttonTestID="rule-card--name-button"
          inputTestID="rule-card--input"
        />
      }
      toggle={
        <SlideToggle
          active={rule.status === 'active'}
          size={ComponentSize.ExtraSmall}
          onChange={onToggle}
          testID="rule-card--slide-toggle"
        />
      }
      // description
      // labels
      disabled={rule.status === 'inactive'}
      contextMenu={
        <NotificationRuleCardContext
          onDelete={onDelete}
          onExport={onExport}
          onClone={onClone}
        />
      }
      metaData={[<>{rule.updatedAt.toString()}</>]}
    />
  )
}

const mdtp: DispatchProps = {
  updateRule: updateRule,
  deleteNotificationRule: deleteRule,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(withRouter(RuleCard))
