// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {SlideToggle, ComponentSize, ResourceCard} from '@influxdata/clockface'
import CheckCardContext from 'src/checks/components/CheckCardContext'
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'
import LastRunTaskStatus from 'src/shared/components/lastRunTaskStatus/LastRunTaskStatus'

// Constants
import {DEFAULT_CHECK_NAME} from 'src/alerting/constants'
import {SEARCH_QUERY_PARAM} from 'src/alerting/constants/history'

// Actions and Selectors
import {
  updateCheckDisplayProperties,
  deleteCheck,
  addCheckLabel,
  deleteCheckLabel,
  cloneCheck,
} from 'src/checks/actions/thunks'
import {viewableLabels} from 'src/labels/selectors'
import {notify} from 'src/shared/actions/notifications'
import {updateCheckFailed} from 'src/shared/copy/notifications'

// Types
import {Check, Label, AppState} from 'src/types'

// Utilities
import {relativeTimestampFormatter} from 'src/shared/utils/relativeTimestampFormatter'

interface DispatchProps {
  onUpdateCheckDisplayProperties: typeof updateCheckDisplayProperties
  deleteCheck: typeof deleteCheck
  onAddCheckLabel: typeof addCheckLabel
  onRemoveCheckLabel: typeof deleteCheckLabel
  onCloneCheck: typeof cloneCheck
  onNotify: typeof notify
}

interface StateProps {
  labels: Label[]
}

interface OwnProps {
  check: Check
}

type Props = OwnProps & DispatchProps & WithRouterProps & StateProps

const CheckCard: FC<Props> = ({
  onRemoveCheckLabel,
  onAddCheckLabel,
  onCloneCheck,
  onNotify,
  check,
  onUpdateCheckDisplayProperties,
  deleteCheck,
  params: {orgID},
  labels,
  router,
}) => {
  const {id, activeStatus, name, description} = check

  const onUpdateName = (name: string) => {
    try {
      onUpdateCheckDisplayProperties(check.id, {name})
    } catch (error) {
      onNotify(updateCheckFailed(error.message))
    }
  }

  const onUpdateDescription = (description: string) => {
    try {
      onUpdateCheckDisplayProperties(check.id, {description})
    } catch (e) {
      onNotify(updateCheckFailed(e.message))
    }
  }

  const onDelete = () => {
    deleteCheck(check.id)
  }

  const onClone = () => {
    onCloneCheck(check)
  }

  const onToggle = () => {
    const status = activeStatus === 'active' ? 'inactive' : 'active'

    try {
      onUpdateCheckDisplayProperties(id, {status})
    } catch (error) {
      onNotify(updateCheckFailed(error.message))
    }
  }

  const onCheckClick = () => {
    router.push(`/orgs/${orgID}/alerting/checks/${id}/edit`)
  }

  const onView = () => {
    const queryParams = new URLSearchParams({
      [SEARCH_QUERY_PARAM]: `"checkID" == "${id}"`,
    })

    router.push(`/orgs/${orgID}/checks/${id}/?${queryParams}`)
  }

  const handleAddCheckLabel = (label: Label) => {
    onAddCheckLabel(id, label)
  }

  const handleRemoveCheckLabel = (label: Label) => {
    onRemoveCheckLabel(id, label.id)
  }

  return (
    <ResourceCard
      key={`check-id--${id}`}
      testID="check-card"
      name={
        <ResourceCard.EditableName
          onUpdate={onUpdateName}
          onClick={onCheckClick}
          name={check.name}
          noNameString={DEFAULT_CHECK_NAME}
          testID="check-card--name"
          buttonTestID="check-card--name-button"
          inputTestID="check-card--input"
        />
      }
      toggle={
        <SlideToggle
          active={activeStatus === 'active'}
          size={ComponentSize.ExtraSmall}
          onChange={onToggle}
          testID="check-card--slide-toggle"
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
          selectedLabels={check.labels as Label[]}
          labels={labels}
          onAddLabel={handleAddCheckLabel}
          onRemoveLabel={handleRemoveCheckLabel}
        />
      }
      disabled={activeStatus === 'inactive'}
      contextMenu={
        <CheckCardContext
          onView={onView}
          onDelete={onDelete}
          onClone={onClone}
        />
      }
      metaData={[
        <>Last completed at {check.latestCompleted}</>,
        <>{relativeTimestampFormatter(check.updatedAt, 'Last updated ')}</>,
        <LastRunTaskStatus
          key={2}
          lastRunError={check.lastRunError}
          lastRunStatus={check.lastRunStatus}
        />,
      ]}
    />
  )
}

const mdtp: DispatchProps = {
  onUpdateCheckDisplayProperties: updateCheckDisplayProperties,
  deleteCheck: deleteCheck,
  onAddCheckLabel: addCheckLabel,
  onRemoveCheckLabel: deleteCheckLabel,
  onCloneCheck: cloneCheck,
  onNotify: notify,
}

const mstp = ({labels}: AppState): StateProps => {
  return {
    labels: viewableLabels(labels.list),
  }
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(CheckCard))
