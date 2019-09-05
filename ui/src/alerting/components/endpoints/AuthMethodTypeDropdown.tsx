// Libraries
import React, {FC} from 'react'

// Components
import {Dropdown} from '@influxdata/clockface'

// Types
import {HTTPAuthMethodType} from 'src/types'

interface AuthMethodType {
  name: string
  type: HTTPAuthMethodType
  id: HTTPAuthMethodType
}

interface Props {
  selectedType: string
  onSelectType: (type: HTTPAuthMethodType) => void
}

const types: AuthMethodType[] = [
  {name: 'none', type: 'none', id: 'none'},
  {name: 'basic', type: 'basic', id: 'basic'},
  {name: 'bearer', type: 'bearer', id: 'bearer'},
]

const AuthMethodTypeDropdown: FC<Props> = ({selectedType, onSelectType}) => {
  const items = types.map(({id, type, name}) => (
    <Dropdown.Item
      key={id}
      id={id}
      value={id}
      testID={`http-authMethod--dropdown-item ${type}`}
      onClick={onSelectType}
    >
      {name}
    </Dropdown.Item>
  ))

  const selected = types.find(t => t.type === selectedType)

  if (!selected) {
    throw new Error(
      'Incorrect authMethod type provided to <AuthMethodTypeDropdown/>'
    )
  }

  const button = (active, onClick) => (
    <Dropdown.Button
      testID="http-authMethod--dropdown--button"
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
      testID="http-authMethod-change--dropdown"
    />
  )
}

export default AuthMethodTypeDropdown
