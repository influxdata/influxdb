// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ResourceList, Context, IconFont} from 'src/clockface'

// Actions
import {deleteTemplate} from 'src/templates/actions'

// Types
import {TemplateSummary} from '@influxdata/influx'
import {ComponentColor} from '@influxdata/clockface'

// Constants
import {DEFAULT_TEMPLATE_NAME} from 'src/templates/constants'

interface OwnProps {
  template: TemplateSummary
  onFilterChange: (searchTerm: string) => void
}

interface DispatchProps {
  onDelete: typeof deleteTemplate
}

type Props = DispatchProps & OwnProps

export class TemplateCard extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {template} = this.props

    return (
      <ResourceList.Card
        testID="template-card"
        contextMenu={() => this.contextMenu}
        name={() => (
          <ResourceList.Name
            onClick={this.handleNameClick}
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
    const {
      template: {id},
      onDelete,
    } = this.props
    return (
      <Context>
        <Context.Menu
          icon={IconFont.Trash}
          color={ComponentColor.Danger}
          testID="context-delete-menu"
        >
          <Context.Item
            label="Delete"
            action={onDelete}
            value={id}
            testID="context-delete-task"
          />
        </Context.Menu>
      </Context>
    )
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

const mdtp: DispatchProps = {
  onDelete: deleteTemplate,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(withRouter<Props>(TemplateCard))
