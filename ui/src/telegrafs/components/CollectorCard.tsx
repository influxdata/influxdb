// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps, Link} from 'react-router'

// Components
import {Context} from 'src/clockface'
import {ResourceCard, IconFont} from '@influxdata/clockface'
import {ComponentColor} from '@influxdata/clockface'
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'

// Actions
import {
  addTelelgrafLabelsAsync,
  removeTelelgrafLabelsAsync,
} from 'src/telegrafs/actions'
import {createLabel as createLabelAsync} from 'src/labels/actions'

// Selectors
import {viewableLabels} from 'src/labels/selectors'

// Constants
import {DEFAULT_COLLECTOR_NAME} from 'src/dashboards/constants'

// Types
import {AppState, Organization} from 'src/types'
import {ILabel, ITelegraf as Telegraf} from '@influxdata/influx'

interface OwnProps {
  collector: Telegraf
  bucket: string
  onDelete: (telegraf: Telegraf) => void
  onUpdate: (telegraf: Telegraf) => void
  onFilterChange: (searchTerm: string) => void
}

interface StateProps {
  labels: ILabel[]
  org: Organization
}

interface DispatchProps {
  onAddLabels: typeof addTelelgrafLabelsAsync
  onRemoveLabels: typeof removeTelelgrafLabelsAsync
  onCreateLabel: typeof createLabelAsync
}

type Props = OwnProps & StateProps & DispatchProps

class CollectorRow extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {collector, bucket, org} = this.props

    return (
      <ResourceCard
        key={`telegraf-id--${collector.id}`}
        testID="resource-card"
        name={
          <ResourceCard.EditableName
            onUpdate={this.handleUpdateName}
            onClick={this.handleNameClick}
            name={collector.name}
            noNameString={DEFAULT_COLLECTOR_NAME}
            testID="collector-card--name"
            buttonTestID="collector-card--name-button"
            inputTestID="collector-card--input"
          />
        }
        description={
          <ResourceCard.EditableDescription
            onUpdate={this.handleUpdateDescription}
            description={collector.description}
            placeholder={`Describe ${collector.name}`}
          />
        }
        labels={this.labels}
        metaData={[
          <>Bucket: {bucket}</>,
          <>
            <Link
              to={`/orgs/${org.id}/load-data/telegrafs/${
                collector.id
              }/instructions`}
              data-testid="setup-instructions-link"
            >
              Setup Instructions
            </Link>
          </>,
        ]}
        contextMenu={this.contextMenu}
      />
    )
  }

  private get contextMenu(): JSX.Element {
    return (
      <Context>
        <Context.Menu icon={IconFont.Trash} color={ComponentColor.Danger}>
          <Context.Item label="Delete" action={this.handleDeleteConfig} />
        </Context.Menu>
      </Context>
    )
  }

  private handleUpdateName = async (name: string) => {
    const {onUpdate, collector} = this.props

    await onUpdate({...collector, name})
  }

  private handleUpdateDescription = (description: string) => {
    const {onUpdate, collector} = this.props

    onUpdate({...collector, description})
  }

  private get labels(): JSX.Element {
    const {collector, labels, onFilterChange} = this.props
    const collectorLabels = viewableLabels(collector.labels)

    return (
      <InlineLabels
        selectedLabels={collectorLabels}
        labels={labels}
        onFilterChange={onFilterChange}
        onAddLabel={this.handleAddLabel}
        onRemoveLabel={this.handleRemoveLabel}
        onCreateLabel={this.handleCreateLabel}
      />
    )
  }

  private handleAddLabel = (label: ILabel): void => {
    const {collector, onAddLabels} = this.props

    onAddLabels(collector.id, [label])
  }

  private handleRemoveLabel = (label: ILabel): void => {
    const {collector, onRemoveLabels} = this.props

    onRemoveLabels(collector.id, [label])
  }

  private handleCreateLabel = async (label: ILabel): Promise<void> => {
    try {
      const {name, properties} = label
      await this.props.onCreateLabel(name, properties)
    } catch (err) {
      throw err
    }
  }

  private handleNameClick = (e: MouseEvent) => {
    e.preventDefault()

    this.handleOpenConfig()
  }

  private handleOpenConfig = (): void => {
    const {collector, router, org} = this.props
    router.push(`/orgs/${org.id}/telegrafs/${collector.id}/view`)
  }

  private handleDeleteConfig = (): void => {
    this.props.onDelete(this.props.collector)
  }
}

const mstp = ({labels, orgs: {org}}: AppState): StateProps => {
  return {org, labels: viewableLabels(labels.list)}
}

const mdtp: DispatchProps = {
  onAddLabels: addTelelgrafLabelsAsync,
  onRemoveLabels: removeTelelgrafLabelsAsync,
  onCreateLabel: createLabelAsync,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<Props>(CollectorRow))
