// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {connect} from 'react-redux'
import {withRouter, RouteComponentProps, Link} from 'react-router-dom'

// Components
import {Context} from 'src/clockface'
import {ResourceCard, IconFont} from '@influxdata/clockface'
import {ComponentColor} from '@influxdata/clockface'
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'

// Actions
import {
  addTelegrafLabelsAsync,
  removeTelegrafLabelsAsync,
} from 'src/telegrafs/actions/thunks'

// Selectors
import {getOrg} from 'src/organizations/selectors'

// Constants
import {DEFAULT_COLLECTOR_NAME} from 'src/dashboards/constants'

// Types
import {AppState, Organization, Label, Telegraf} from 'src/types'

interface OwnProps {
  collector: Telegraf
  onDelete: (telegraf: Telegraf) => void
  onUpdate: (telegraf: Telegraf) => void
  onFilterChange: (searchTerm: string) => void
}

interface StateProps {
  org: Organization
}

interface DispatchProps {
  onAddLabels: typeof addTelegrafLabelsAsync
  onRemoveLabels: typeof removeTelegrafLabelsAsync
}

type Props = OwnProps & StateProps & DispatchProps

class CollectorRow extends PureComponent<
  Props & RouteComponentProps<{orgID: string}>
> {
  public render() {
    const {collector, org} = this.props

    return (
      <ResourceCard
        key={`telegraf-id--${collector.id}`}
        testID="resource-card"
        contextMenu={this.contextMenu}
      >
        <ResourceCard.EditableName
          onUpdate={this.handleUpdateName}
          onClick={this.handleNameClick}
          name={collector.name}
          noNameString={DEFAULT_COLLECTOR_NAME}
          testID="collector-card--name"
          buttonTestID="collector-card--name-button"
          inputTestID="collector-card--input"
        />
        <ResourceCard.EditableDescription
          onUpdate={this.handleUpdateDescription}
          description={collector.description}
          placeholder={`Describe ${collector.name}`}
        />
        <ResourceCard.Meta>
          <span key={`bucket-key--${collector.id}`} data-testid="bucket-name">
            {/* todo(glinton): verify what sets this. It seems like it is using the 'config' section of 'influxdb_v2' output?? */}
            Bucket: {collector.metadata.buckets.join(', ')}
          </span>
          <Link
            to={`/orgs/${org.id}/load-data/telegrafs/${collector.id}/instructions`}
            data-testid="setup-instructions-link"
          >
            Setup Instructions
          </Link>
        </ResourceCard.Meta>
        {this.labels}
      </ResourceCard>
    )
  }

  private get contextMenu(): JSX.Element {
    return (
      <Context>
        <Context.Menu
          testID="telegraf-delete-menu"
          icon={IconFont.Trash}
          color={ComponentColor.Danger}
        >
          <Context.Item
            testID="telegraf-delete-button"
            label="Delete"
            action={this.handleDeleteConfig}
          />
        </Context.Menu>
      </Context>
    )
  }

  private handleUpdateName = (name: string) => {
    const {onUpdate, collector} = this.props

    onUpdate({...collector, name})
  }

  private handleUpdateDescription = (description: string) => {
    const {onUpdate, collector} = this.props

    onUpdate({...collector, description})
  }

  private get labels(): JSX.Element {
    const {collector, onFilterChange} = this.props

    return (
      <InlineLabels
        selectedLabelIDs={collector.labels}
        onFilterChange={onFilterChange}
        onAddLabel={this.handleAddLabel}
        onRemoveLabel={this.handleRemoveLabel}
      />
    )
  }

  private handleAddLabel = async (label: Label) => {
    const {collector, onAddLabels} = this.props

    await onAddLabels(collector.id, [label])
  }

  private handleRemoveLabel = async (label: Label) => {
    const {collector, onRemoveLabels} = this.props

    await onRemoveLabels(collector.id, [label])
  }

  private handleNameClick = (e: MouseEvent) => {
    e.preventDefault()

    this.handleOpenConfig()
  }

  private handleOpenConfig = (): void => {
    const {collector, history, org} = this.props
    history.push(`/orgs/${org.id}/load-data/telegrafs/${collector.id}/view`)
  }

  private handleDeleteConfig = (): void => {
    this.props.onDelete(this.props.collector)
  }
}

const mstp = (state: AppState): StateProps => {
  const org = getOrg(state)

  return {org}
}

const mdtp: DispatchProps = {
  onAddLabels: addTelegrafLabelsAsync,
  onRemoveLabels: removeTelegrafLabelsAsync,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter(CollectorRow))
