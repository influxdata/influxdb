import React, {Component} from 'react'
import classnames from 'classnames'

import TemplateControlDropdown from 'src/tempVars/components/TemplateControlDropdown'
import OverlayTechnology from 'src/reusable_ui/components/overlays/OverlayTechnology'
import TemplateVariableEditor from 'src/tempVars/components/TemplateVariableEditor'
import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'

import {Template, Source} from 'src/types'

interface Props {
  meRole: string
  isUsingAuth: boolean
  templates: Template[]
  isOpen: boolean
  source: Source
  onPickTemplate: (id: string) => void
  onSaveTemplates: (templates: Template[]) => void
}

interface State {
  isAdding: boolean
}

class TemplateControlBar extends Component<Props, State> {
  constructor(props) {
    super(props)

    this.state = {isAdding: false}
  }

  public render() {
    const {
      isOpen,
      templates,
      onPickTemplate,
      meRole,
      isUsingAuth,
      source,
    } = this.props
    const {isAdding} = this.state

    return (
      <div className={classnames('template-control-bar', {show: isOpen})}>
        <div className="template-control--container">
          <div className="template-control--controls">
            {templates && templates.length ? (
              templates.map(template => (
                <TemplateControlDropdown
                  key={template.id}
                  meRole={meRole}
                  isUsingAuth={isUsingAuth}
                  template={template}
                  templates={templates}
                  source={source}
                  onPickTemplate={onPickTemplate}
                  onCreateTemplate={this.handleCreateTemplate}
                  onUpdateTemplate={this.handleUpdateTemplate}
                  onDeleteTemplate={this.handleDeleteTemplate}
                />
              ))
            ) : (
              <div className="template-control--empty" data-test="empty-state">
                This dashboard does not have any{' '}
                <strong>Template Variables</strong>
              </div>
            )}
            <OverlayTechnology visible={isAdding}>
              <TemplateVariableEditor
                templates={templates}
                source={source}
                onCreate={this.handleCreateTemplate}
                onCancel={this.handleCancelAddVariable}
              />
            </OverlayTechnology>
          </div>
          <Authorized requiredRole={EDITOR_ROLE}>
            <button
              className="btn btn-primary btn-sm template-control--manage"
              data-test="add-template-variable"
              onClick={this.handleAddVariable}
            >
              <span className="icon plus" />
              Add Variable
            </button>
          </Authorized>
        </div>
      </div>
    )
  }

  private handleAddVariable = (): void => {
    this.setState({isAdding: true})
  }

  private handleCancelAddVariable = (): void => {
    this.setState({isAdding: false})
  }

  private handleCreateTemplate = async (template: Template): Promise<void> => {
    const {templates, onSaveTemplates} = this.props

    await onSaveTemplates([...templates, template])

    this.setState({isAdding: false})
  }

  private handleUpdateTemplate = async (template: Template): Promise<void> => {
    const {templates, onSaveTemplates} = this.props
    const newTemplates = templates.reduce((acc, t) => {
      if (t.id === template.id) {
        return [...acc, template]
      }

      return [...acc, t]
    }, [])

    await onSaveTemplates(newTemplates)
  }

  private handleDeleteTemplate = async (template: Template): Promise<void> => {
    const {templates, onSaveTemplates} = this.props
    const newTemplates = templates.filter(t => t.id !== template.id)

    await onSaveTemplates(newTemplates)
  }
}

export default TemplateControlBar
