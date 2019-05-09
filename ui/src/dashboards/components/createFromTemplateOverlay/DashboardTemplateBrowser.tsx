// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {TemplateSummary, Template} from 'src/types'
import DashboardTemplateDetails from 'src/dashboards/components/createFromTemplateOverlay/DashboardTemplateDetails'
import DashboardTemplateList from 'src/dashboards/components/createFromTemplateOverlay/DashboardTemplateList'

interface Props {
  templates: TemplateSummary[]
  selectedTemplateSummary: TemplateSummary
  selectedTemplate: Template
  variables: string[]
  cells: string[]
  onSelectTemplate: (selectedTemplateSummary: TemplateSummary) => void
}

class DashboardTemplateBrowser extends PureComponent<Props> {
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
        <DashboardTemplateList
          templates={templates}
          onSelectTemplate={onSelectTemplate}
          selectedTemplateSummary={selectedTemplateSummary}
        />
        <DashboardTemplateDetails
          cells={cells}
          variables={variables}
          selectedTemplateSummary={selectedTemplateSummary}
          selectedTemplate={selectedTemplate}
        />
      </div>
    )
  }
}

export default DashboardTemplateBrowser
