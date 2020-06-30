// Libraries
import React, {FC, Dispatch} from 'react'
import {withRouter, WithRouterProps} from 'react-router-dom'
import {connect} from 'react-redux'

// Actions
import {
  addEndpointLabel,
  deleteEndpointLabel,
  deleteEndpoint,
  updateEndpointProperties,
  cloneEndpoint,
} from 'src/notifications/endpoints/actions/thunks'

// Components
import {
  SlideToggle,
  ComponentSize,
  ResourceCard,
  FlexDirection,
  AlignItems,
  FlexBox,
} from '@influxdata/clockface'
import EndpointCardMenu from 'src/notifications/endpoints/components/EndpointCardMenu'
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'

// Constants
import {
  SEARCH_QUERY_PARAM,
  HISTORY_TYPE_QUERY_PARAM,
} from 'src/alerting/constants/history'

// Types
import {NotificationEndpoint, Label, AlertHistoryType} from 'src/types'
import {Action} from 'src/notifications/endpoints/actions/creators'

// Utilities
import {relativeTimestampFormatter} from 'src/shared/utils/relativeTimestampFormatter'

interface DispatchProps {
  onDeleteEndpoint: typeof deleteEndpoint
  onAddEndpointLabel: typeof addEndpointLabel
  onRemoveEndpointLabel: typeof deleteEndpointLabel
  onUpdateEndpointProperties: typeof updateEndpointProperties
  onCloneEndpoint: typeof cloneEndpoint
}

interface OwnProps {
  endpoint: NotificationEndpoint
}

interface DispatchProp {
  dispatch: Dispatch<Action>
}

type Props = OwnProps & WithRouterProps & DispatchProps & DispatchProp

const EndpointCard: FC<Props> = ({
  router,
  params: {orgID},
  endpoint,
  onUpdateEndpointProperties,
  onCloneEndpoint,
  onDeleteEndpoint,
  onAddEndpointLabel,
  onRemoveEndpointLabel,
}) => {
  const {id, name, description, activeStatus} = endpoint

  const handleUpdateName = (name: string) => {
    onUpdateEndpointProperties(id, {name})
  }

  const handleClick = () => {
    router.push(`orgs/${orgID}/alerting/endpoints/${id}/edit`)
  }

  const handleToggle = () => {
    const toStatus = activeStatus === 'active' ? 'inactive' : 'active'
    onUpdateEndpointProperties(id, {status: toStatus})
  }

  const handleView = () => {
    const historyType: AlertHistoryType = 'notifications'

    const queryParams = new URLSearchParams({
      [HISTORY_TYPE_QUERY_PARAM]: historyType,
      [SEARCH_QUERY_PARAM]: `"notificationEndpointID" == "${id}"`,
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

  const handleAddEndpointLabel = (label: Label) => {
    onAddEndpointLabel(id, label)
  }
  const handleRemoveEndpointLabel = (label: Label) => {
    onRemoveEndpointLabel(id, label.id)
  }

  const handleUpdateDescription = (description: string) => {
    onUpdateEndpointProperties(id, {description})
  }

  return (
    <ResourceCard
      key={id}
      contextMenu={contextMenu}
      disabled={activeStatus === 'inactive'}
      direction={FlexDirection.Row}
      alignItems={AlignItems.Center}
      margin={ComponentSize.Large}
      testID={`endpoint-card ${name}`}
    >
      <SlideToggle
        active={activeStatus === 'active'}
        size={ComponentSize.ExtraSmall}
        onChange={handleToggle}
        testID="endpoint-card--slide-toggle"
      />
      <FlexBox
        direction={FlexDirection.Column}
        alignItems={AlignItems.FlexStart}
        margin={ComponentSize.Small}
      >
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
        <ResourceCard.EditableDescription
          onUpdate={handleUpdateDescription}
          description={description}
          placeholder={`Describe ${name}`}
        />
        <ResourceCard.Meta>
          <>{relativeTimestampFormatter(endpoint.updatedAt, 'Last updated ')}</>
        </ResourceCard.Meta>
        <InlineLabels
          selectedLabelIDs={endpoint.labels}
          onAddLabel={handleAddEndpointLabel}
          onRemoveLabel={handleRemoveEndpointLabel}
        />
      </FlexBox>
    </ResourceCard>
  )
}

const mdtp: DispatchProps = {
  onDeleteEndpoint: deleteEndpoint,
  onAddEndpointLabel: addEndpointLabel,
  onRemoveEndpointLabel: deleteEndpointLabel,
  onUpdateEndpointProperties: updateEndpointProperties,
  onCloneEndpoint: cloneEndpoint,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(withRouter<OwnProps>(EndpointCard))
