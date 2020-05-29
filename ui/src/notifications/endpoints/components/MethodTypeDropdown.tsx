// Libraries
import React, {FC} from 'react'

// Components
import {Dropdown} from '@influxdata/clockface'

// Types
import {HTTPMethodType} from 'src/types'

interface MethodType {
  name: string
  type: HTTPMethodType
  id: HTTPMethodType
}

interface Props {
  selectedType: string
  onSelectType: (type: HTTPMethodType) => void
}

const types: MethodType[] = [{name: 'POST', type: 'POST', id: 'POST'}]

const MethodTypeDropdown: FC<Props> = ({selectedType, onSelectType}) => {
  const items = types.map(({id, type, name}) => (
    <Dropdown.Item
      key={id}
      id={id}
      value={id}
      testID={`http-method--dropdown-item ${type}`}
      onClick={onSelectType}
    >
      {name}
    </Dropdown.Item>
  ))

  const selected = types.find(t => t.type === selectedType)

  if (!selected) {
    throw new Error('Incorrect method type provided to <MethodTypeDropdown/>')
  }

  const button = (active, onClick) => (
    <Dropdown.Button
      testID="http-method--dropdown--button"
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
      testID="http-method-change--dropdown"
    />
  )
}

export default MethodTypeDropdown
