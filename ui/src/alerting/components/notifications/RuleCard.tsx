// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {SlideToggle, ComponentSize, ResourceCard} from '@influxdata/clockface'
import NotificationRuleCardContext from 'src/alerting/components/notifications/RuleCardContext'
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'
import LastRunTaskStatus from 'src/shared/components/lastRunTaskStatus/LastRunTaskStatus'

// Constants
import {DEFAULT_NOTIFICATION_RULE_NAME} from 'src/alerting/constants'
import {
  SEARCH_QUERY_PARAM,
  HISTORY_TYPE_QUERY_PARAM,
} from 'src/alerting/constants/history'

// Actions and Selectors
import {
  updateRuleProperties,
  deleteRule,
  addRuleLabel,
  deleteRuleLabel,
  cloneRule,
} from 'src/alerting/actions/notifications/rules'
import {viewableLabels} from 'src/labels/selectors'
import {createLabel as createLabelAsync} from 'src/labels/actions'

// Types
import {
  NotificationRuleDraft,
  AppState,
  Label,
  AlertHistoryType,
} from 'src/types'

// Utilities
import {relativeTimestampFormatter} from 'src/shared/utils/relativeTimestampFormatter'

interface DispatchProps {
  onUpdateRuleProperties: typeof updateRuleProperties
  deleteNotificationRule: typeof deleteRule
  onAddRuleLabel: typeof addRuleLabel
  onRemoveRuleLabel: typeof deleteRuleLabel
  onCreateLabel: typeof createLabelAsync
  onCloneRule: typeof cloneRule
}

interface OwnProps {
  rule: NotificationRuleDraft
}

interface StateProps {
  labels: Label[]
}

type Props = OwnProps & WithRouterProps & StateProps & DispatchProps

const RuleCard: FC<Props> = ({
  rule,
  onUpdateRuleProperties,
  labels,
  deleteNotificationRule,
  onCloneRule,
  onAddRuleLabel,
  onRemoveRuleLabel,
  onCreateLabel,
  params: {orgID},
  router,
}) => {
  const onUpdateName = (name: string) => {
    onUpdateRuleProperties(rule.id, {name})
  }

  const onUpdateDescription = (description: string) => {
    onUpdateRuleProperties(rule.id, {description})
  }

  const onDelete = () => {
    deleteNotificationRule(rule.id)
  }

  const onClone = () => {
    onCloneRule(rule)
  }

  const onToggle = () => {
    const status = rule.status === 'active' ? 'inactive' : 'active'

    onUpdateRuleProperties(rule.id, {status})
  }

  const onRuleClick = () => {
    router.push(`/orgs/${orgID}/alerting/rules/${rule.id}/edit`)
  }

  const onView = () => {
    const historyType: AlertHistoryType = 'notifications'

    const queryParams = new URLSearchParams({
      [HISTORY_TYPE_QUERY_PARAM]: historyType,
      [SEARCH_QUERY_PARAM]: `"notificationRuleID" == "${rule.id}"`,
    })

    router.push(`/orgs/${orgID}/alert-history?${queryParams}`)
  }

  const handleAddRuleLabel = (label: Label) => {
    onAddRuleLabel(rule.id, label)
  }

  const handleRemoveRuleLabel = (label: Label) => {
    onRemoveRuleLabel(rule.id, label)
  }

  const handleCreateLabel = (label: Label) => {
    onCreateLabel(label.name, label.properties)
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
      description={
        <ResourceCard.EditableDescription
          onUpdate={onUpdateDescription}
          description={rule.description}
          placeholder={`Describe ${rule.name}`}
        />
      }
      labels={
        <InlineLabels
          selectedLabels={rule.labels as Label[]}
          labels={labels}
          onAddLabel={handleAddRuleLabel}
          onRemoveLabel={handleRemoveRuleLabel}
          onCreateLabel={handleCreateLabel}
        />
      }
      disabled={rule.status === 'inactive'}
      contextMenu={
        <NotificationRuleCardContext
          onView={onView}
          onClone={onClone}
          onDelete={onDelete}
        />
      }
      metaData={[
        <>Last completed at {rule.latestCompleted}</>,
        <>{relativeTimestampFormatter(rule.updatedAt, 'Last updated ')}</>,
        <LastRunTaskStatus
          key={2}
          lastRunError={rule.lastRunError}
          lastRunStatus={rule.lastRunStatus}
        />,
      ]}
    />
  )
}

const mdtp: DispatchProps = {
  onUpdateRuleProperties: updateRuleProperties,
  deleteNotificationRule: deleteRule,
  onCreateLabel: createLabelAsync,
  onAddRuleLabel: addRuleLabel,
  onRemoveRuleLabel: deleteRuleLabel,
  onCloneRule: cloneRule,
}

const mstp = ({labels}: AppState): StateProps => {
  return {
    labels: viewableLabels(labels.list),
  }
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(RuleCard))
