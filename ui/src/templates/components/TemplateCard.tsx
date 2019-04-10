// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ResourceList, Context, IconFont} from 'src/clockface'
import InlineLabels, {
  LabelsEditMode,
} from 'src/shared/components/inlineLabels/InlineLabels'

// Actions
import {
  deleteTemplate,
  cloneTemplate,
  updateTemplate,
} from 'src/templates/actions'
// Selectors
import {viewableLabels} from 'src/labels/selectors'

// Types
import {TemplateSummary, ILabel} from '@influxdata/influx'
import {ComponentColor} from '@influxdata/clockface'
import {AppState, Organization} from 'src/types'

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
}

interface StateProps {
  labels: ILabel[]
  org: Organization
}

type Props = DispatchProps & OwnProps & StateProps

class TemplateCard extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {template, labels, onFilterChange} = this.props

    return (
      <ResourceList.Card
        testID="template-card"
        contextMenu={() => this.contextMenu}
        name={() => (
          <ResourceList.Name
            onClick={this.handleNameClick}
            onUpdate={this.handleUpdateTemplate}
            name={template.meta.name}
            noNameString={DEFAULT_TEMPLATE_NAME}
            parentTestID="template-card--name"
            buttonTestID="template-card--name-button"
            inputTestID="template-card--input"
          />
        )}
        labels={() => (
          <InlineLabels
            selectedLabels={template.labels}
            labels={labels}
            onFilterChange={onFilterChange}
            editMode={LabelsEditMode.Readonly}
          />
        )}
      />
    )
  }

  private handleUpdateTemplate = (name: string) => {
    const {template} = this.props

    this.props.onUpdate(template.id, {
      ...template,
      meta: {...template.meta, name},
    })
  }

  private get contextMenu(): JSX.Element {
    const {
      template: {id},
      onDelete,
    } = this.props
    return (
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
    )
  }

  private handleClone = () => {
    const {
      template: {id},
      onClone,
    } = this.props
    onClone(id)
  }

  private handleNameClick = (e: MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault()
    this.handleViewTemplate()
  }

  private handleViewTemplate = () => {
    const {router, template, org} = this.props
    router.push(`/orgs/${org.id}/templates/${template.id}/view`)
  }
}

const mstp = ({labels, orgs: {org}}: AppState): StateProps => {
  return {
    org,
    labels: viewableLabels(labels.list),
  }
}

const mdtp: DispatchProps = {
  onDelete: deleteTemplate,
  onClone: cloneTemplate,
  onUpdate: updateTemplate,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<Props>(TemplateCard))
