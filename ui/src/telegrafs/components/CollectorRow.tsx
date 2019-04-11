// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {IndexList, Alignment, ConfirmationButton} from 'src/clockface'
import {
  ComponentSize,
  Button,
  ComponentColor,
  ComponentSpacer,
  FlexDirection,
  JustifyContent,
  AlignItems,
} from '@influxdata/clockface'
import {ITelegraf as Telegraf, Organization} from '@influxdata/influx'
import EditableName from 'src/shared/components/EditableName'
import EditableDescription from 'src/shared/components/editable_description/EditableDescription'
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'

// Actions
import {
  addTelelgrafLabelsAsync,
  removeTelelgrafLabelsAsync,
} from 'src/telegrafs/actions'
import {createLabel as createLabelAsync} from 'src/labels/actions'
import {
  setTelegrafConfigID,
  setTelegrafConfigName,
} from 'src/dataLoaders/actions/dataLoaders'

// Selectors
import {viewableLabels} from 'src/labels/selectors'

// Constants
import {DEFAULT_COLLECTOR_NAME} from 'src/dashboards/constants'

// Types
import {AppState} from 'src/types'
import {ILabel} from '@influxdata/influx'

interface OwnProps {
  collector: Telegraf
  bucket: string
  onDelete: (telegraf: Telegraf) => void
  onUpdate: (telegraf: Telegraf) => void
  onOpenInstructions: (telegrafID: string) => void
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
  onSetTelegrafConfigID: typeof setTelegrafConfigID
  onSetTelegrafConfigName: typeof setTelegrafConfigName
}

type Props = OwnProps & StateProps & DispatchProps

class CollectorRow extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {collector, bucket} = this.props

    return (
      <>
        <IndexList.Row>
          <IndexList.Cell>
            <ComponentSpacer
              margin={ComponentSize.Small}
              direction={FlexDirection.Column}
              alignItems={AlignItems.FlexStart}
              stretchToFitWidth={true}
            >
              <EditableName
                onUpdate={this.handleUpdateName}
                name={collector.name}
                noNameString={DEFAULT_COLLECTOR_NAME}
                onEditName={this.handleNameClick}
              />
              <EditableDescription
                description={collector.description}
                placeholder={`Describe ${collector.name}`}
                onUpdate={this.handleUpdateDescription}
              />
              {this.labels}
            </ComponentSpacer>
          </IndexList.Cell>
          <IndexList.Cell>{bucket}</IndexList.Cell>
          <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
            <ComponentSpacer
              margin={ComponentSize.Small}
              direction={FlexDirection.Row}
              justifyContent={JustifyContent.FlexEnd}
            >
              <Button
                text="Setup Instructions"
                size={ComponentSize.ExtraSmall}
                color={ComponentColor.Secondary}
                onClick={this.handleOpenInstructions}
              />
              <ConfirmationButton
                size={ComponentSize.ExtraSmall}
                text="Delete"
                confirmText="Confirm"
                onConfirm={this.handleDeleteConfig}
              />
            </ComponentSpacer>
          </IndexList.Cell>
        </IndexList.Row>
      </>
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

  private handleNameClick = (e: MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault()
    this.handleOpenConfig()
  }

  private handleOpenConfig = (): void => {
    const {
      collector,
      router,
      org,
      onSetTelegrafConfigID,
      onSetTelegrafConfigName,
    } = this.props
    onSetTelegrafConfigID(collector.id)
    onSetTelegrafConfigName(collector.name)
    router.push(`/orgs/${org.id}/telegrafs/${collector.id}/view`)
  }

  private handleDeleteConfig = (): void => {
    this.props.onDelete(this.props.collector)
  }

  private handleOpenInstructions = (): void => {
    this.props.onOpenInstructions(this.props.collector.id)
  }
}

const mstp = ({labels, orgs: {org}}: AppState): StateProps => {
  return {org, labels: viewableLabels(labels.list)}
}

const mdtp: DispatchProps = {
  onAddLabels: addTelelgrafLabelsAsync,
  onRemoveLabels: removeTelelgrafLabelsAsync,
  onCreateLabel: createLabelAsync,
  onSetTelegrafConfigID: setTelegrafConfigID,
  onSetTelegrafConfigName: setTelegrafConfigName,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<Props>(CollectorRow))
