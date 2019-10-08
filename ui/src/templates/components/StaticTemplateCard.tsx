// Libraries
import React, {PureComponent} from 'react'
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
import OverlayLink from 'src/overlays/components/OverlayLink'

// Actions
import {createResourceFromStaticTemplate} from 'src/templates/actions'

// Selectors
import {viewableLabels} from 'src/labels/selectors'

// Types
import {TemplateSummary, ILabel} from '@influxdata/influx'
import {ComponentColor} from '@influxdata/clockface'
import {AppState, Organization} from 'src/types'

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
  labels: ILabel[]
  org: Organization
}

type Props = DispatchProps & OwnProps & StateProps

class StaticTemplateCard extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {template} = this.props

    const templateID = template.meta.name.replace(/\s/gi, '-').toLowerCase()

    return (
      <ResourceCard
        testID="template-card"
        contextMenu={this.contextMenu}
        description={this.description}
        name={
          <OverlayLink overlayID="view-static-template" resourceID={templateID}>
            {onClick => (
              <ResourceCard.Name
                onClick={onClick}
                name={template.meta.name}
                testID="template-card--name"
              />
            )}
          </OverlayLink>
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
}

const mstp = ({labels, orgs: {org}}: AppState): StateProps => {
  return {
    org,
    labels: viewableLabels(labels.list),
  }
}

const mdtp: DispatchProps = {
  onCreateFromTemplate: createResourceFromStaticTemplate,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<Props>(StaticTemplateCard))
