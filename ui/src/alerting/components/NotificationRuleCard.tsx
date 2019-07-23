// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ResourceList} from 'src/clockface'
import {SlideToggle, ComponentSize} from '@influxdata/clockface'
import NotificationRuleCardContext from 'src/alerting/components/NotificationRuleCardContext'

// Constants
import {DEFAULT_NOTIFICATION_RULE_NAME} from 'src/alerting/constants'

// Actions
import {
  updateNotificationRule,
  deleteNotificationRule,
} from 'src/alerting/actions/notificationRules'

// Types
import {NotificationRule, NotificationRuleBase} from 'src/types'

interface DispatchProps {
  updateNotificationRule: typeof updateNotificationRule
  deleteNotificationRule: typeof deleteNotificationRule
}

interface OwnProps {
  notificationRule: NotificationRule
}

type Props = OwnProps & DispatchProps & WithRouterProps

const NotificationRuleCard: FunctionComponent<Props> = ({
  notificationRule,
  updateNotificationRule,
  deleteNotificationRule,
  router,
  params: {orgID},
}) => {
  const onUpdateName = (name: string) => {
    updateNotificationRule({id: notificationRule.id, name})
  }

  const onClickName = () => {
    router.push(`/orgs/${orgID}/notificationRules/${notificationRule.id}`)
  }

  const onDelete = () => {
    deleteNotificationRule(notificationRule.id)
  }

  const onExport = () => {}

  const onClone = () => {}

  const onToggle = () => {
    const status =
      notificationRule.status == NotificationRuleBase.StatusEnum.Active
        ? NotificationRuleBase.StatusEnum.Inactive
        : NotificationRuleBase.StatusEnum.Active
    updateNotificationRule({id: notificationRule.id, status})
  }

  return (
    <ResourceList.Card
      key={`notificationRule-id--${notificationRule.id}`}
      testID="notificationRule-card"
      name={() => (
        <ResourceList.EditableName
          onUpdate={onUpdateName}
          onClick={onClickName}
          name={notificationRule.name}
          noNameString={DEFAULT_NOTIFICATION_RULE_NAME}
          parentTestID="notificationRule-card--name"
          buttonTestID="notificationRule-card--name-button"
          inputTestID="notificationRule-card--input"
        />
      )}
      toggle={() => (
        <SlideToggle
          active={
            notificationRule.status == NotificationRuleBase.StatusEnum.Active
          }
          size={ComponentSize.ExtraSmall}
          onChange={onToggle}
          testID="notificationRule-card--slide-toggle"
        />
      )}
      // description
      // labels
      disabled={
        notificationRule.status == NotificationRuleBase.StatusEnum.Inactive
      }
      contextMenu={() => (
        <NotificationRuleCardContext
          onDelete={onDelete}
          onExport={onExport}
          onClone={onClone}
        />
      )}
      updatedAt={notificationRule.updatedAt.toString()}
    />
  )
}

const mdtp: DispatchProps = {
  updateNotificationRule: updateNotificationRule,
  deleteNotificationRule: deleteNotificationRule,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(withRouter(NotificationRuleCard))
