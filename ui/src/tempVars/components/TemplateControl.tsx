import React, {PureComponent} from 'react'

import OverlayTechnology from 'src/reusable_ui/components/overlays/OverlayTechnology'
import TemplateDropdown from 'src/tempVars/components/TemplateDropdown'
import TemplateVariableEditor from 'src/tempVars/components/TemplateVariableEditor'
import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'
import {calculateDropdownWidth} from 'src/dashboards/constants/templateControlBar'

import {Template, Source, TemplateValue} from 'src/types'

interface Props {
  template: Template
  templates: Template[]
  meRole: string
  isUsingAuth: boolean
  source: Source
  onPickValue: (v: TemplateValue) => void
  onCreateTemplate: (template: Template) => Promise<void>
  onUpdateTemplate: (template: Template) => Promise<void>
  onDeleteTemplate: (template: Template) => Promise<void>
}

interface State {
  isEditing: boolean
}

class TemplateControl extends PureComponent<Props, State> {
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
      onCreateTemplate,
      onPickValue,
    } = this.props
    const {isEditing} = this.state

    const dropdownStyle = template.values.length
      ? {minWidth: calculateDropdownWidth(template.values)}
      : null

    return (
      <div className="template-control--dropdown" style={dropdownStyle}>
        <TemplateDropdown template={template} onPickValue={onPickValue} />
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

export default TemplateControl
