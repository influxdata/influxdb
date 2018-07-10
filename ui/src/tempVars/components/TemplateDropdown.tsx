import React, {SFC} from 'react'

import Dropdown from 'src/shared/components/Dropdown'

import {Template, TemplateValueType} from 'src/types'

interface Props {
  template: Template
  onPickTemplate: (id: string) => void
}

const TemplateDropdown: SFC<Props> = props => {
  const {template, onPickTemplate} = props

  const dropdownItems = template.values.map(value => {
    if (value.type === TemplateValueType.Map) {
      return {...value, text: value.key}
    }
    return {...value, text: value.value}
  })

  const localSelectedItem = dropdownItems.find(item => item.localSelected) ||
    dropdownItems[0] || {text: '(No values)'}

  return (
    <Dropdown
      items={dropdownItems}
      buttonSize="btn-xs"
      menuClass="dropdown-astronaut"
      useAutoComplete={true}
      selected={localSelectedItem.text}
      onChoose={onPickTemplate(template.id)}
    />
  )
}

export default TemplateDropdown
