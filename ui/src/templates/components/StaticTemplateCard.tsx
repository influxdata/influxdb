// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {get, capitalize} from 'lodash'
import {connect, ConnectedProps} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'
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
import {createResourceFromStaticTemplate} from 'src/templates/actions/thunks'

// Selectors
import {getOrg} from 'src/organizations/selectors'

// Types
import {ComponentColor} from '@influxdata/clockface'
import {AppState, TemplateSummary} from 'src/types'

// Constants
interface OwnProps {
  template: TemplateSummary
  name: string
  onFilterChange: (searchTerm: string) => void
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps & OwnProps

class StaticTemplateCard extends PureComponent<
  Props & RouteComponentProps<{orgID: string}>
> {
  public render() {
    const {template} = this.props

    return (
      <ResourceCard testID="template-card" contextMenu={this.contextMenu}>
        <ResourceCard.Name
          onClick={this.handleNameClick}
          name={template.meta.name}
          testID="template-card--name"
        />
        {this.description}
        <ResourceCard.Meta>
          {capitalize(get(template, 'content.data.type', ''))}
        </ResourceCard.Meta>
      </ResourceCard>
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
    const description = get(template, 'content.data.attributes.description')

    return (
      <ResourceCard.Description description={description || 'No description'} />
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
    const {history, org, name} = this.props

    history.push(`/orgs/${org.id}/settings/templates/${name}/static/view`)
  }
}

const mstp = (state: AppState) => {
  return {
    org: getOrg(state),
  }
}

const mdtp = {
  onCreateFromTemplate: createResourceFromStaticTemplate,
}

const connector = connect(mstp, mdtp)

export default connector(withRouter(StaticTemplateCard))
