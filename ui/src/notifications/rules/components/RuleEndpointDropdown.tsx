// Libraries
import React, {FC} from 'react'
import {get} from 'lodash'

// Components
import {Dropdown, ComponentStatus} from '@influxdata/clockface'

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
  if (!endpoints.length) {
    const button = () => (
      <Dropdown.Button status={ComponentStatus.Disabled} onClick={() => {}}>
        No Endpoints Found
      </Dropdown.Button>
    )

    const menu = () => null

    return <Dropdown button={button} style={{width: '160px'}} menu={menu} />
  }

  const items = endpoints.map(({id, name}) => (
    <Dropdown.Item
      key={id}
      id={id}
      value={id}
      testID={`endpoint--dropdown-item ${id}`}
      onClick={() => onSelectEndpoint(id)}
    >
      {name}
    </Dropdown.Item>
  ))

  const selectedEndpoint =
    endpoints.find(e => e.id === selectedEndpointID) || endpoints[0]

  const button = (active, onClick) => (
    <Dropdown.Button
      testID="endpoint--dropdown--button"
      active={active}
      onClick={onClick}
    >
      {get(selectedEndpoint, 'name')}
    </Dropdown.Button>
  )

  const menu = onCollapse => (
    <Dropdown.Menu onCollapse={onCollapse}>{items}</Dropdown.Menu>
  )

  return (
    <Dropdown
      button={button}
      menu={menu}
      style={{width: '160px'}}
      testID="endpoint-change--dropdown"
    />
  )
}

export default RuleEndpointDropdown
