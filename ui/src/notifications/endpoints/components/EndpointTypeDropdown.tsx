// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {Dropdown} from '@influxdata/clockface'

// Utils
import {extractBlockedEndpoints} from 'src/cloud/utils/limits'

// Types
import {NotificationEndpointType, AppState} from 'src/types'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

interface EndpointType {
  id: NotificationEndpointType
  type: NotificationEndpointType
  name: string
  flag?: string // feature flag that enable/disable endpoint type
}

interface StateProps {
  blockedEndpoints: string[]
}

interface OwnProps {
  selectedType: string
  onSelectType: (type: NotificationEndpointType) => void
}

type Props = OwnProps & StateProps

const types: EndpointType[] = [
  {name: 'HTTP', type: 'http', id: 'http'},
  {name: 'Slack', type: 'slack', id: 'slack'},
  {name: 'Pagerduty', type: 'pagerduty', id: 'pagerduty'},
  {
    name: 'Telegram',
    type: 'telegram',
    id: 'telegram',
    flag: 'notification-endpoint-telegram',
  },
]

const EndpointTypeDropdown: FC<Props> = ({
  selectedType,
  onSelectType,
  blockedEndpoints,
}) => {
  const items = types
    .filter(
      ({type, flag}) =>
        !blockedEndpoints.includes(type) && (!flag || isFlagEnabled(flag))
    )
    .map(({id, type, name}) => (
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
    <Dropdown button={button} menu={menu} testID="endpoint-change--dropdown" />
  )
}

const mstp = ({cloud: {limits}}: AppState) => {
  return {
    blockedEndpoints: extractBlockedEndpoints(limits),
  }
}

export default connect<StateProps>(mstp)(EndpointTypeDropdown)
