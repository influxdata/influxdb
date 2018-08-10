import React, {PureComponent} from 'react'

import OverlayTechnology from 'src/reusable_ui/components/overlays/OverlayTechnology'
import TemplateDropdown from 'src/tempVars/components/TemplateDropdown'
import TextTemplateSelector from 'src/tempVars/components/TextTemplateSelector'
import TemplateVariableEditor from 'src/tempVars/components/TemplateVariableEditor'
import {calculateDropdownWidth} from 'src/dashboards/constants/templateControlBar'

import {Template, TemplateType, Source, TemplateValue} from 'src/types'

interface Props {
  template: Template
  templates: Template[]
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
        {template.type === TemplateType.Text ? (
          <TextTemplateSelector
            template={template}
            onPickValue={onPickValue}
            key={template.id}
          />
        ) : (
          <TemplateDropdown template={template} onPickValue={onPickValue} />
        )}

        <label className="template-control--label">{template.tempVar}</label>

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
