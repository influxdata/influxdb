// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {SlideToggle, ComponentSize, ResourceCard} from '@influxdata/clockface'
import NotificationRuleCardContext from 'src/notifications/rules/components/RuleCardContext'
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
} from 'src/notifications/rules/actions/thunks'

// Types
import {NotificationRuleDraft, Label, AlertHistoryType} from 'src/types'

// Utilities
import {relativeTimestampFormatter} from 'src/shared/utils/relativeTimestampFormatter'

interface DispatchProps {
  onUpdateRuleProperties: typeof updateRuleProperties
  deleteNotificationRule: typeof deleteRule
  onAddRuleLabel: typeof addRuleLabel
  onRemoveRuleLabel: typeof deleteRuleLabel
  onCloneRule: typeof cloneRule
}

interface OwnProps {
  rule: NotificationRuleDraft
}

type Props = OwnProps & WithRouterProps & DispatchProps

const RuleCard: FC<Props> = ({
  rule,
  onUpdateRuleProperties,
  deleteNotificationRule,
  onCloneRule,
  onAddRuleLabel,
  onRemoveRuleLabel,
  params: {orgID},
  router,
}) => {
  const {
    id,
    activeStatus,
    name,
    lastRunError,
    lastRunStatus,
    description,
    latestCompleted,
  } = rule

  const onUpdateName = (name: string) => {
    onUpdateRuleProperties(id, {name})
  }

  const onUpdateDescription = (description: string) => {
    onUpdateRuleProperties(id, {description})
  }

  const onDelete = () => {
    deleteNotificationRule(id)
  }

  const onClone = () => {
    onCloneRule(rule)
  }

  const onToggle = () => {
    const status = activeStatus === 'active' ? 'inactive' : 'active'

    onUpdateRuleProperties(id, {status})
  }

  const onRuleClick = () => {
    router.push(`/orgs/${orgID}/alerting/rules/${id}/edit`)
  }

  const onView = () => {
    const historyType: AlertHistoryType = 'notifications'

    const queryParams = new URLSearchParams({
      [HISTORY_TYPE_QUERY_PARAM]: historyType,
      [SEARCH_QUERY_PARAM]: `"notificationRuleID" == "${id}"`,
    })

    router.push(`/orgs/${orgID}/alert-history?${queryParams}`)
  }

  const handleAddRuleLabel = (label: Label) => {
    onAddRuleLabel(id, label)
  }

  const handleRemoveRuleLabel = (label: Label) => {
    onRemoveRuleLabel(id, label.id)
  }

  return (
    <ResourceCard
      key={`rule-id--${id}`}
      testID={`rule-card ${name}`}
      name={
        <ResourceCard.EditableName
          onUpdate={onUpdateName}
          onClick={onRuleClick}
          name={name}
          noNameString={DEFAULT_NOTIFICATION_RULE_NAME}
          testID="rule-card--name"
          buttonTestID="rule-card--name-button"
          inputTestID="rule-card--input"
        />
      }
      toggle={
        <SlideToggle
          active={activeStatus === 'active'}
          size={ComponentSize.ExtraSmall}
          onChange={onToggle}
          testID="rule-card--slide-toggle"
        />
      }
      description={
        <ResourceCard.EditableDescription
          onUpdate={onUpdateDescription}
          description={description}
          placeholder={`Describe ${name}`}
        />
      }
      labels={
        <InlineLabels
          selectedLabelIDs={rule.labels}
          onAddLabel={handleAddRuleLabel}
          onRemoveLabel={handleRemoveRuleLabel}
        />
      }
      disabled={activeStatus === 'inactive'}
      contextMenu={
        <NotificationRuleCardContext
          onView={onView}
          onClone={onClone}
          onDelete={onDelete}
        />
      }
      metaData={[
        <>Last completed at {latestCompleted}</>,
        <>{relativeTimestampFormatter(rule.updatedAt, 'Last updated ')}</>,
        <LastRunTaskStatus
          key={2}
          lastRunError={lastRunError}
          lastRunStatus={lastRunStatus}
        />,
      ]}
    />
  )
}

const mdtp: DispatchProps = {
  onUpdateRuleProperties: updateRuleProperties,
  deleteNotificationRule: deleteRule,
  onAddRuleLabel: addRuleLabel,
  onRemoveRuleLabel: deleteRuleLabel,
  onCloneRule: cloneRule,
}

export default connect<{}, DispatchProps>(
  null,
  mdtp
)(withRouter(RuleCard))
