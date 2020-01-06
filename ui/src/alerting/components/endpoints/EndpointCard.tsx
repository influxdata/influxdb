/* eslint no-console: 0 */

// Libraries
import React, {FC, Dispatch} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Actions and Selectors
import {createLabel as createLabelAsync} from 'src/labels/actions'
import {viewableLabels} from 'src/labels/selectors'
import {
  addEndpointLabel,
  deleteEndpointLabel,
  deleteEndpoint,
  updateEndpointProperties,
  cloneEndpoint,
} from 'src/alerting/actions/notifications/endpoints'

// Components
import {SlideToggle, ComponentSize, ResourceCard} from '@influxdata/clockface'
import EndpointCardMenu from 'src/alerting/components/endpoints/EndpointCardMenu'
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'

// Constants
import {
  SEARCH_QUERY_PARAM,
  HISTORY_TYPE_QUERY_PARAM,
} from 'src/alerting/constants/history'

// Types
import {
  NotificationEndpoint,
  Label,
  AppState,
  AlertHistoryType,
} from 'src/types'
import {Action} from 'src/alerting/actions/notifications/endpoints'

// Utilities
import {relativeTimestampFormatter} from 'src/shared/utils/relativeTimestampFormatter'

interface DispatchProps {
  onDeleteEndpoint: typeof deleteEndpoint
  onAddEndpointLabel: typeof addEndpointLabel
  onRemoveEndpointLabel: typeof deleteEndpointLabel
  onCreateLabel: typeof createLabelAsync
  onUpdateEndpointProperties: typeof updateEndpointProperties
  onCloneEndpoint: typeof cloneEndpoint
}

interface StateProps {
  labels: Label[]
}

interface OwnProps {
  endpoint: NotificationEndpoint
}

interface DispatchProp {
  dispatch: Dispatch<Action>
}

type Props = OwnProps &
  WithRouterProps &
  DispatchProps &
  StateProps &
  DispatchProp

const EndpointCard: FC<Props> = ({
  labels,
  router,
  params: {orgID},
  endpoint,
  onCreateLabel,
  onUpdateEndpointProperties,
  onCloneEndpoint,
  onDeleteEndpoint,
  onAddEndpointLabel,
  onRemoveEndpointLabel,
}) => {
  const {id, name, status, description} = endpoint

  const handleUpdateName = (name: string) => {
    onUpdateEndpointProperties(id, {name})
  }
  const handleClick = () => {
    router.push(`orgs/${orgID}/alerting/endpoints/${endpoint.id}/edit`)
  }

  const nameComponent = (
    <ResourceCard.EditableName
      key={id}
      name={name}
      onClick={handleClick}
      onUpdate={handleUpdateName}
      testID={`endpoint-card--name ${name}`}
      inputTestID="endpoint-card--input"
      buttonTestID="endpoint-card--name-button"
      noNameString="Name this notification endpoint"
    />
  )

  const handleToggle = () => {
    const toStatus = status === 'active' ? 'inactive' : 'active'
    onUpdateEndpointProperties(id, {status: toStatus})
  }
  const toggle = (
    <SlideToggle
      active={status === 'active'}
      size={ComponentSize.ExtraSmall}
      onChange={handleToggle}
      testID="endpoint-card--slide-toggle"
    />
  )

  const handleView = () => {
    const historyType: AlertHistoryType = 'notifications'

    const queryParams = new URLSearchParams({
      [HISTORY_TYPE_QUERY_PARAM]: historyType,
      [SEARCH_QUERY_PARAM]: `"notificationEndpointID" == "${endpoint.id}"`,
    })

    router.push(`/orgs/${orgID}/alert-history?${queryParams}`)
  }
  const handleDelete = () => {
    onDeleteEndpoint(id)
  }
  const handleClone = () => {
    onCloneEndpoint(endpoint)
  }
  const contextMenu = (
    <EndpointCardMenu
      onDelete={handleDelete}
      onView={handleView}
      onClone={handleClone}
    />
  )

  const handleCreateLabel = (label: Label) => {
    onCreateLabel(label.name, label.properties)
  }
  const handleAddEndpointLabel = (label: Label) => {
    onAddEndpointLabel(id, label)
  }
  const handleRemoveEndpointLabel = (label: Label) => {
    onRemoveEndpointLabel(id, label)
  }
  const labelsComponent = (
    <InlineLabels
      selectedLabels={endpoint.labels as Label[]}
      labels={labels}
      onAddLabel={handleAddEndpointLabel}
      onRemoveLabel={handleRemoveEndpointLabel}
      onCreateLabel={handleCreateLabel}
    />
  )

  const handleUpdateDescription = (description: string) => {
    onUpdateEndpointProperties(id, {description})
  }
  const descriptionComponent = (
    <ResourceCard.EditableDescription
      onUpdate={handleUpdateDescription}
      description={description}
      placeholder={`Describe ${name}`}
    />
  )

  return (
    <ResourceCard
      key={id}
      toggle={toggle}
      name={nameComponent}
      contextMenu={contextMenu}
      description={descriptionComponent}
      labels={labelsComponent}
      disabled={status === 'inactive'}
      metaData={[
        <>{relativeTimestampFormatter(endpoint.updatedAt, 'Last updated ')}</>,
      ]}
      testID={`endpoint-card ${name}`}
    />
  )
}

const mdtp: DispatchProps = {
  onDeleteEndpoint: deleteEndpoint,
  onCreateLabel: createLabelAsync,
  onAddEndpointLabel: addEndpointLabel,
  onRemoveEndpointLabel: deleteEndpointLabel,
  onUpdateEndpointProperties: updateEndpointProperties,
  onCloneEndpoint: cloneEndpoint,
}

const mstp = ({labels}: AppState): StateProps => ({
  labels: viewableLabels(labels.list),
})

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<OwnProps>(EndpointCard))
