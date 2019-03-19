// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ResourceList} from 'src/clockface'

// Actions

// Types
import {TemplateSummary} from '@influxdata/influx'

// Constants
import {DEFAULT_TEMPLATE_NAME} from 'src/templates/constants'

interface Props {
  template: TemplateSummary
  onFilterChange: (searchTerm: string) => void
}

export class TemplateCard extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {template} = this.props

    return (
      <ResourceList.Card
        testID="template-card"
        contextMenu={() => this.contextMenu}
        name={() => (
          <ResourceList.Name
            onEditName={this.handleNameClick}
            onUpdate={this.doNothing}
            name={template.meta.name}
            noNameString={DEFAULT_TEMPLATE_NAME}
            parentTestID="template-card--name"
            buttonTestID="template-card--name-button"
            inputTestID="template-card--input"
          />
        )}
      />
    )
  }

  //TODO handle rename template
  private doNothing = () => {}

  private get contextMenu(): JSX.Element {
    return null
  }

  private handleNameClick = (e: MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault()
    this.handleExport()
  }

  private handleExport = () => {
    const {
      router,
      template,
      params: {orgID},
    } = this.props
    router.push(`organizations/${orgID}/templates/${template.id}/export`)
  }
}

export default withRouter<Props>(TemplateCard)
