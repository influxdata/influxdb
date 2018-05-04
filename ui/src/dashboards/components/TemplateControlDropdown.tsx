import React, {SFC} from 'react'

import Dropdown from 'src/shared/components/Dropdown'
import {calculateDropdownWidth} from 'src/dashboards/constants/templateControlBar'
import {isUserAuthorized, EDITOR_ROLE} from 'src/auth/Authorized'
import {Template} from 'src/types/dashboard'

interface Props {
  template: Template
  meRole: string
  isUsingAuth: boolean
  onSelectTemplate: (id: string) => void
}

// TODO: change Dropdown to a MultiSelectDropdown, `selected` to
// the full array, and [item] to all `selected` values when we update
// this component to support multiple values

const TemplateControlDropdown: SFC<Props> = ({
  template,
  onSelectTemplate,
  isUsingAuth,
  meRole,
}) => {
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
        disabled={isUsingAuth && !isUserAuthorized(meRole, EDITOR_ROLE)}
        onChoose={onSelectTemplate(template.id)}
      />
      <label className="template-control--label">{template.tempVar}</label>
    </div>
  )
}

export default TemplateControlDropdown
