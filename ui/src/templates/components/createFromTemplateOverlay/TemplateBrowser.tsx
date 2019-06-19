// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {TemplateSummary, Template} from 'src/types'
import TemplateBrowserDetails from 'src/templates/components/createFromTemplateOverlay/TemplateBrowserDetails'
import TemplateBrowserList from 'src/templates/components/createFromTemplateOverlay/TemplateBrowserList'

interface Props {
  templates: TemplateSummary[]
  selectedTemplateSummary: TemplateSummary
  selectedTemplate: Template
  variables?: string[]
  cells?: string[]
  onSelectTemplate: (selectedTemplateSummary: TemplateSummary) => void
}

class TemplateBrowser extends PureComponent<Props> {
  public render() {
    const {
      selectedTemplateSummary,
      cells,
      variables,
      selectedTemplate,
      templates,
      onSelectTemplate,
    } = this.props

    return (
      <div className="import-template-overlay">
        <TemplateBrowserList
          templates={templates}
          onSelectTemplate={onSelectTemplate}
          selectedTemplateSummary={selectedTemplateSummary}
        />
        <TemplateBrowserDetails
          cells={cells}
          variables={variables}
          selectedTemplateSummary={selectedTemplateSummary}
          selectedTemplate={selectedTemplate}
        />
      </div>
    )
  }
}

export default TemplateBrowser
