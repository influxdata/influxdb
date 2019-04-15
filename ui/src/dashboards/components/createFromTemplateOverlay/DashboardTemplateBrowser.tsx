// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ResponsiveGridSizer} from 'src/clockface'
import {TemplateSummary, ITemplate} from '@influxdata/influx'
import CardSelectCard from 'src/clockface/components/card_select/CardSelectCard'
import DashboardTemplateDetails from 'src/dashboards/components/createFromTemplateOverlay/DashboardTemplateDetails'

// Types
import GetResources, {
  ResourceTypes,
} from 'src/configuration/components/GetResources'

interface Props {
  templates: TemplateSummary[]
  selectedTemplateSummary: TemplateSummary
  selectedTemplate: ITemplate
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
    } = this.props

    return (
      <div className="import-template-overlay">
        <GetResources resource={ResourceTypes.Templates}>
          <ResponsiveGridSizer columns={2}>
            {templates.map(t => (
              <CardSelectCard
                key={t.id}
                id={t.id}
                onClick={this.handleCardClick(t)}
                checked={_.get(selectedTemplateSummary, 'id', '') === t.id}
                label={t.meta.name}
                hideImage={true}
                testID={`card-select-${t.meta.name}`}
              />
            ))}
          </ResponsiveGridSizer>
        </GetResources>
        <DashboardTemplateDetails
          cells={cells}
          variables={variables}
          selectedTemplateSummary={selectedTemplateSummary}
          selectedTemplate={selectedTemplate}
        />
      </div>
    )
  }

  private handleCardClick = (template: TemplateSummary) => (): void => {
    const {onSelectTemplate} = this.props

    onSelectTemplate(template)
  }
}

export default DashboardTemplateBrowser
