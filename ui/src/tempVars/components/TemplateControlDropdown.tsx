import React, {PureComponent} from 'react'

import Dropdown from 'src/shared/components/Dropdown'
import SimpleOverlayTechnology from 'src/shared/components/SimpleOverlayTechnology'
import TemplateVariableEditor from 'src/tempVars/components/TemplateVariableEditor'
import {calculateDropdownWidth} from 'src/dashboards/constants/templateControlBar'
import Authorized, {isUserAuthorized, EDITOR_ROLE} from 'src/auth/Authorized'

import {Template, Source} from 'src/types'

interface Props {
  template: Template
  meRole: string
  isUsingAuth: boolean
  source: Source
  onSelectTemplate: (id: string) => void
  onCreateTemplate: (template: Template) => Promise<void>
  onUpdateTemplate: (template: Template) => Promise<void>
  onDeleteTemplate: (template: Template) => Promise<void>
}

interface State {
  isEditing: boolean
}

class TemplateControlDropdown extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      isEditing: false,
    }
  }

  public render() {
    const {
      template,
      isUsingAuth,
      meRole,
      source,
      onSelectTemplate,
      onCreateTemplate,
    } = this.props
    const {isEditing} = this.state

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
        <Authorized requiredRole={EDITOR_ROLE}>
          <label className="template-control--label">
            {template.tempVar}
            <span
              className="icon cog-thick"
              onClick={this.handleShowSettings}
            />
          </label>
        </Authorized>
        {isEditing && (
          <SimpleOverlayTechnology>
            <TemplateVariableEditor
              template={template}
              source={source}
              onCreate={onCreateTemplate}
              onUpdate={this.handleUpdateTemplate}
              onDelete={this.handleDelete}
              onCancel={this.handleHideSettings}
            />
          </SimpleOverlayTechnology>
        )}
      </div>
    )
  }

  private handleShowSettings = (): void => {
    this.setState({isEditing: true})
  }

  private handleHideSettings = (): void => {
    this.setState({isEditing: false})
  }

  private handleUpdateTemplate = async (template: Template): Promise<void> => {
    const {onUpdateTemplate} = this.props

    await onUpdateTemplate(template)

    this.setState({isEditing: false})
  }

  private handleDelete = (): Promise<any> => {
    const {onDeleteTemplate, template} = this.props

    return onDeleteTemplate(template)
  }
}

export default TemplateControlDropdown
