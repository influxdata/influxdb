import React, {SFC} from 'react'

import Dropdown from 'src/shared/components/Dropdown'

import {Template, TemplateValue, TemplateValueType} from 'src/types'

interface Props {
  template: Template
  onPickValue: (v: TemplateValue) => void
}

const TemplateDropdown: SFC<Props> = props => {
  const {template, onPickValue} = props

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
      onChoose={onPickValue}
    />
  )
}

export default TemplateDropdown
