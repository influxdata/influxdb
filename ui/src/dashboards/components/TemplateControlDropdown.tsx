import React, {SFC} from 'react'

import Dropdown from 'src/shared/components/Dropdown'
import {calculateDropdownWidth} from 'src/dashboards/constants/templateControlBar'
import {Template} from 'src/types/dashboard'

interface Props {
  template: Template
  onSelectTemplate: (id: string) => void
}

// TODO: change Dropdown to a MultiSelectDropdown, `selected` to
// the full array, and [item] to all `selected` values when we update
// this component to support multiple values

const TemplateControlDropdown: SFC<Props> = ({template, onSelectTemplate}) => {
  const dropdownItems = template.values.map(value => ({
    ...value,
    text: value.value,
  }))

  const dropdownStyle = template.values.length
    ? {minWidth: calculateDropdownWidth(template.values)}
    : null

  const selectedItem = dropdownItems.find(item => item.selected) ||
    dropdownItems[0] || {text: '(No values)'}

  return (
    <div className="template-control--dropdown" style={dropdownStyle}>
      <Dropdown
        items={dropdownItems}
        buttonSize="btn-xs"
        menuClass="dropdown-astronaut"
        useAutoComplete={true}
        selected={selectedItem.text}
        onChoose={onSelectTemplate(template.id)}
      />
      <label className="template-control--label">{template.tempVar}</label>
    </div>
  )
}

export default TemplateControlDropdown
