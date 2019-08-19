// Libraries
import React, {FC} from 'react'

// Components
import {Dropdown} from '@influxdata/clockface'

// Types
import {NotificationEndpointType} from 'src/types'

interface EndpointType {
  id: NotificationEndpointType
  type: NotificationEndpointType
  name: string
}

interface Props {
  selectedType: string
  onSelectType: (type: NotificationEndpointType) => void
}

const types: EndpointType[] = [
  {name: 'Slack', type: 'slack', id: 'slack'},
  {name: 'Pagerduty', type: 'pagerduty', id: 'pagerduty'},
]

const EndpointTypeDropdown: FC<Props> = ({selectedType, onSelectType}) => {
  const items = types.map(({id, type, name}) => (
    <Dropdown.Item
      key={id}
      id={id}
      value={id}
      testID={`endpoint--dropdown-item ${type}`}
      onClick={onSelectType}
    >
      {name}
    </Dropdown.Item>
  ))

  const selected = types.find(t => t.type === selectedType)

  if (!selected) {
    throw new Error(
      'Incorrect endpoint type provided to <EndpointTypeDropdown/>'
    )
  }

  const button = (active, onClick) => (
    <Dropdown.Button
      testID="endpoint--dropdown--button"
      active={active}
      onClick={onClick}
    >
      {selected.name}
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
      testID="endpoint-change--dropdown"
    />
  )
}

export default EndpointTypeDropdown
