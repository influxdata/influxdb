/* eslint no-console: 0 */

// Libraries
import React, {FC} from 'react'

// Components
import {SlideToggle, ComponentSize, ResourceCard} from '@influxdata/clockface'
import EndpointCardMenu from 'src/alerting/components/endpoints/EndpointCardMenu'

// Types
import {NotificationEndpoint} from 'src/types'

interface OwnProps {
  endpoint: NotificationEndpoint
}

type Props = OwnProps

const EndpointCard: FC<Props> = ({endpoint}) => {
  const {id, name, status} = endpoint

  const handleUpdateName = () => console.trace('implement update endpoint name')
  const handleClick = () => console.trace('implement click endpoint name')

  const nameComponent = (
    <ResourceCard.EditableName
      key={id}
      name={name}
      onClick={handleClick}
      onUpdate={handleUpdateName}
      testID="endpoint-card--name"
      inputTestID="endpoint-card--input"
      buttonTestID="endpoint-card--name-button"
      noNameString="Name this notification endpoint"
    />
  )

  const handleToggle = () => console.trace('implement toggle status')
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

export default EndpointCard
