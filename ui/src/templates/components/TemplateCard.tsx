// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {connect} from 'react-redux'
import {get, capitalize} from 'lodash'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {
  Button,
  ComponentSize,
  FlexBox,
  FlexDirection,
  JustifyContent,
} from '@influxdata/clockface'

// Components
import {Context} from 'src/clockface'
import {ResourceCard, IconFont} from '@influxdata/clockface'
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'

// Actions
import {
  deleteTemplate,
  cloneTemplate,
  updateTemplate,
  createResourceFromTemplate,
  removeTemplateLabelsAsync,
  addTemplateLabelsAsync,
} from 'src/templates/actions/thunks'

// Selectors
import {getOrg} from 'src/organizations/selectors'

// Types
import {ComponentColor} from '@influxdata/clockface'
import {AppState, Organization, Label, TemplateSummary} from 'src/types'

// Constants
import {DEFAULT_TEMPLATE_NAME} from 'src/templates/constants'

interface OwnProps {
  template: TemplateSummary
  onFilterChange: (searchTerm: string) => void
}

interface DispatchProps {
  onDelete: typeof deleteTemplate
  onClone: typeof cloneTemplate
  onUpdate: typeof updateTemplate
  onCreateFromTemplate: typeof createResourceFromTemplate
  onAddTemplateLabels: typeof addTemplateLabelsAsync
  onRemoveTemplateLabels: typeof removeTemplateLabelsAsync
}

interface StateProps {
  org: Organization
}

type Props = DispatchProps & OwnProps & StateProps

class TemplateCard extends PureComponent<
  Props & RouteComponentProps<{orgID: string}>
> {
  public render() {
    const {template, onFilterChange} = this.props

    return (
      <ResourceCard testID="template-card" contextMenu={this.contextMenu}>
        <ResourceCard.EditableName
          onClick={this.handleNameClick}
          onUpdate={this.handleUpdateTemplateName}
          name={template.meta.name}
          noNameString={DEFAULT_TEMPLATE_NAME}
          testID="template-card--name"
          buttonTestID="template-card--name-button"
          inputTestID="template-card--input"
        />
        {this.description}
        <ResourceCard.Meta>
          {capitalize(get(template, 'content.data.type', ''))}
        </ResourceCard.Meta>
        <InlineLabels
          selectedLabelIDs={template.labels}
          onFilterChange={onFilterChange}
          onAddLabel={this.handleAddLabel}
          onRemoveLabel={this.handleRemoveLabel}
        />
      </ResourceCard>
    )
  }

  private handleUpdateTemplateName = (name: string) => {
    const {template} = this.props

    this.props.onUpdate(template.id, {
      ...template,
      meta: {...template.meta, name},
    })
  }

  private handleUpdateTemplateDescription = (description: string) => {
    const {template} = this.props

    this.props.onUpdate(template.id, {
      ...template,
      meta: {...template.meta, description},
    })
  }

  private get description(): JSX.Element {
    const {template} = this.props
    const description = get(template, 'meta.description', '')
    const name = get(template, 'meta.name', '')

    return (
      <ResourceCard.EditableDescription
        onUpdate={this.handleUpdateTemplateDescription}
        description={description}
        placeholder={`Describe ${name} Template`}
      />
    )
  }

  private get contextMenu(): JSX.Element {
    const {
      template: {id},
      onDelete,
    } = this.props
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
        <Context>
          <Context.Menu
            icon={IconFont.Duplicate}
            color={ComponentColor.Secondary}
          >
            <Context.Item label="Clone" action={this.handleClone} value={id} />
          </Context.Menu>
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
      </FlexBox>
    )
  }

  private handleCreate = () => {
    const {onCreateFromTemplate, template} = this.props

    onCreateFromTemplate(template.id)
  }

  private handleClone = () => {
    const {
      template: {id},
      onClone,
    } = this.props
    onClone(id)
  }

  private handleNameClick = (e: MouseEvent): void => {
    e.preventDefault()

    this.handleViewTemplate()
  }

  private handleViewTemplate = () => {
    const {history, template, org} = this.props
    history.push(`/orgs/${org.id}/settings/templates/${template.id}/view`)
  }

  private handleAddLabel = (label: Label): void => {
    const {template, onAddTemplateLabels} = this.props

    onAddTemplateLabels(template.id, [label])
  }

  private handleRemoveLabel = (label: Label): void => {
    const {template, onRemoveTemplateLabels} = this.props

    onRemoveTemplateLabels(template.id, [label])
  }
}

const mstp = (state: AppState): StateProps => {
  return {
    org: getOrg(state),
  }
}

const mdtp: DispatchProps = {
  onDelete: deleteTemplate,
  onClone: cloneTemplate,
  onUpdate: updateTemplate,
  onCreateFromTemplate: createResourceFromTemplate,
  onAddTemplateLabels: addTemplateLabelsAsync,
  onRemoveTemplateLabels: removeTemplateLabelsAsync,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter(TemplateCard))
