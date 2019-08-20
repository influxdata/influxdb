/* eslint no-console: 0 */

// Libraries
import React, {FC, Dispatch} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {SlideToggle, ComponentSize, ResourceCard} from '@influxdata/clockface'
import EndpointCardMenu from 'src/alerting/components/endpoints/EndpointCardMenu'

// Types
import {NotificationEndpoint} from 'src/types'
import {Action} from 'src/alerting/actions/notifications/endpoints'

interface OwnProps {
  endpoint: NotificationEndpoint
}

interface DispatchProps {
  dispatch: Dispatch<Action>
}

type Props = OwnProps & WithRouterProps & DispatchProps

const EndpointCard: FC<Props> = ({endpoint, router, params, dispatch}) => {
  const {id, name, status} = endpoint
  const {orgID} = params

  const handleUpdateName = (name: string) => {
    dispatch({
      type: 'SET_ENDPOINT',
      endpoint: {
        ...endpoint,
        name,
      },
    })
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
    dispatch({
      type: 'SET_ENDPOINT',
      endpoint: {
        ...endpoint,
        status: status === 'active' ? 'inactive' : 'active',
      },
    })
  }

  const toggle = (
    <SlideToggle
      active={status === 'active'}
      size={ComponentSize.ExtraSmall}
      onChange={handleToggle}
      testID="endpoint-card--slide-toggle"
    />
  )

  const handleDelete = () => console.trace('implement delete')
  const handleExport = () => console.trace('implement export')
  const handleClone = () => console.trace('implement delete')
  const contextMenu = (
    <EndpointCardMenu
      onDelete={handleDelete}
      onExport={handleExport}
      onClone={handleClone}
    />
  )

  return (
    <ResourceCard
      key={id}
      toggle={toggle}
      name={nameComponent}
      contextMenu={contextMenu}
      disabled={status === 'inactive'}
      metaData={[<>{endpoint.updatedAt}</>]}
      testID={`endpoint-card ${name}`}
    />
  )
}

export default connect()(withRouter<OwnProps>(EndpointCard))
