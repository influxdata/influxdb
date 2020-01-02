// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'
import {
  Button,
  ComponentSize,
  FlexBox,
  FlexDirection,
  JustifyContent,
} from '@influxdata/clockface'

// Components
import {ResourceCard} from '@influxdata/clockface'

// Actions
import {createResourceFromStaticTemplate} from 'src/templates/actions'

// Selectors
import {viewableLabels} from 'src/labels/selectors'
import {getOrg} from 'src/organizations/selectors'

// Types
import {TemplateSummary} from '@influxdata/influx'
import {ComponentColor} from '@influxdata/clockface'
import {AppState, Organization, Label} from 'src/types'

// Constants
interface OwnProps {
  template: TemplateSummary
  name: string
  onFilterChange: (searchTerm: string) => void
}

interface DispatchProps {
  onCreateFromTemplate: typeof createResourceFromStaticTemplate
}

interface StateProps {
  labels: Label[]
  org: Organization
}

type Props = DispatchProps & OwnProps & StateProps

class StaticTemplateCard extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {template} = this.props

    return (
      <ResourceCard
        testID="template-card"
        contextMenu={this.contextMenu}
        description={this.description}
        name={
          <ResourceCard.Name
            onClick={this.handleNameClick}
            name={template.meta.name}
            testID="template-card--name"
          />
        }
        metaData={[this.templateType]}
      />
    )
  }

  private get contextMenu(): JSX.Element {
    return (
      <FlexBox
        margin={ComponentSize.Medium}
        direction={FlexDirection.Row}
        justifyContent={JustifyContent.FlexEnd}
      >
        <Button
          text="Create"
          color={ComponentColor.Primary}
          size={ComponentSize.ExtraSmall}
          onClick={this.handleCreate}
        />
      </FlexBox>
    )
  }

  private get description(): JSX.Element {
    const {template} = this.props
    const description = _.get(template, 'content.data.attributes.description')

    return (
      <ResourceCard.Description description={description || 'No description'} />
    )
  }

  private get templateType(): JSX.Element {
    const {template} = this.props

    return (
      <div className="resource-list--meta-item">
        {_.get(template, 'content.data.type')}
      </div>
    )
  }

  private handleCreate = () => {
    const {onCreateFromTemplate, name} = this.props

    onCreateFromTemplate(name)
  }

  private handleNameClick = (e: MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault()
    this.handleViewTemplate()
  }

  private handleViewTemplate = () => {
    const {router, org, name} = this.props

    router.push(`/orgs/${org.id}/settings/templates/${name}/static/view`)
  }
}

const mstp = (state: AppState): StateProps => {
  return {
    org: getOrg(state),
    labels: viewableLabels(state.labels.list),
  }
}

const mdtp: DispatchProps = {
  onCreateFromTemplate: createResourceFromStaticTemplate,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<Props>(StaticTemplateCard))
