import React, {PureComponent} from 'react'

import Dropdown from 'src/shared/components/Dropdown'
import OverlayTechnology from 'src/reusable_ui/components/overlays/OverlayTechnology'
import TemplateVariableEditor from 'src/tempVars/components/TemplateVariableEditor'
import {calculateDropdownWidth} from 'src/dashboards/constants/templateControlBar'
import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'

import {Template, Source, TemplateValueType} from 'src/types'

interface Props {
  template: Template
  templates: Template[]
  meRole: string
  isUsingAuth: boolean
  source: Source
  onPickTemplate: (id: string) => void
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
      templates,
      source,
      onPickTemplate,
      onCreateTemplate,
    } = this.props
    const {isEditing} = this.state

    const dropdownItems = template.values.map(value => {
      if (value.type === TemplateValueType.Map) {
        return {...value, text: value.key}
      }
      return {...value, text: value.value}
    })

    const dropdownStyle = template.values.length
      ? {minWidth: calculateDropdownWidth(template.values)}
      : null

    const localSelectedItem = dropdownItems.find(item => item.localSelected) ||
      dropdownItems[0] || {text: '(No values)'}

    return (
      <div className="template-control--dropdown" style={dropdownStyle}>
        <Dropdown
          items={dropdownItems}
          buttonSize="btn-xs"
          menuClass="dropdown-astronaut"
          useAutoComplete={true}
          selected={localSelectedItem.text}
          onChoose={onPickTemplate(template.id)}
        />
        <Authorized requiredRole={EDITOR_ROLE}>
          <label className="template-control--label">
            {template.tempVar}
            <span
              className="icon cog-thick"
              onClick={this.handleShowSettings}
              data-test="edit"
            />
          </label>
        </Authorized>
        <OverlayTechnology visible={isEditing}>
          <TemplateVariableEditor
            template={template}
            templates={templates}
            source={source}
            onCreate={onCreateTemplate}
            onUpdate={this.handleUpdateTemplate}
            onDelete={this.handleDelete}
            onCancel={this.handleHideSettings}
          />
        </OverlayTechnology>
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
