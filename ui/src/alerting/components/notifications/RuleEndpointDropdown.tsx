// Libraries
import React, {FC} from 'react'

// Components
import {Dropdown} from '@influxdata/clockface'

// Types
import {NotificationEndpoint} from 'src/types'

interface Props {
  selectedEndpointID: string
  endpoints: NotificationEndpoint[]
  onSelectEndpoint: (endpointID: string) => void
}

const RuleEndpointDropdown: FC<Props> = ({
  endpoints,
  selectedEndpointID,
  onSelectEndpoint,
}) => {
  const items = endpoints.map(({id, type, name}) => (
    <Dropdown.Item
      key={id}
      id={id}
      value={id}
      testID={`endpoint--dropdown-item ${type}`}
      onClick={() => onSelectEndpoint(id)}
    >
      {name}
    </Dropdown.Item>
  ))

  const selectedEndpoint = endpoints.find(e => e.id === selectedEndpointID)

  const button = (active, onClick) => (
    <Dropdown.Button
      testID="endpoint--dropdown--button"
      active={active}
      onClick={onClick}
    >
      {selectedEndpoint.name}
    </Dropdown.Button>
  )

  const menu = onCollapse => (
    <Dropdown.Menu onCollapse={onCollapse}>{items}</Dropdown.Menu>
  )

  return (
    <Dropdown
      button={button}
      menu={menu}
      widthPixels={160}
      testID="status-change--dropdown"
    />
  )
}

export default RuleEndpointDropdown
