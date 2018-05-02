import React, {SFC} from 'react'

import classnames from 'classnames'
import uuid from 'uuid'

import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'
import TemplateControlDropdown from 'src/dashboards/components/TemplateControlDropdown'
import {Template} from 'src/types/dashboard'

interface Props {
  meRole: string
  isUsingAuth: boolean
  templates: Template[]
  isOpen: boolean
  onOpenTemplateManager: () => void
  onSelectTemplate: (id: string) => void
}

const TemplateControlBar: SFC<Props> = ({
  isOpen,
  templates,
  onSelectTemplate,
  onOpenTemplateManager,
  meRole,
  isUsingAuth,
}) => (
  <div className={classnames('template-control-bar', {show: isOpen})}>
    <div className="template-control--container">
      <div className="template-control--controls">
        {templates && templates.length ? (
          templates.map(template => (
            <TemplateControlDropdown
              key={uuid.v4()}
              meRole={meRole}
              isUsingAuth={isUsingAuth}
              template={template}
              onSelectTemplate={onSelectTemplate}
            />
          ))
        ) : (
          <div className="template-control--empty" data-test="empty-state">
            This dashboard does not have any <strong>Template Variables</strong>
          </div>
        )}
      </div>
      <Authorized requiredRole={EDITOR_ROLE}>
        <button
          className="btn btn-primary btn-sm template-control--manage"
          onClick={onOpenTemplateManager}
        >
          <span className="icon cog-thick" />
          Manage
        </button>
      </Authorized>
    </div>
  </div>
)

export default TemplateControlBar
