// Libraries
import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import CollectorList from 'src/organizations/components/CollectorList'
import TelegrafExplainer from 'src/organizations/components/TelegrafExplainer'
import TelegrafInstructionsOverlay from 'src/organizations/components/TelegrafInstructionsOverlay'
import TelegrafConfigOverlay from 'src/organizations/components/TelegrafConfigOverlay'
import {
  Button,
  ComponentColor,
  IconFont,
  ComponentSize,
  Columns,
} from '@influxdata/clockface'
import {EmptyState, Grid, Input, InputType, Tabs} from 'src/clockface'
import CollectorsWizard from 'src/dataLoaders/components/collectorsWizard/CollectorsWizard'
import FilterList from 'src/shared/components/Filter'

// APIS
import {client} from 'src/utils/api'

// Actions
import * as NotificationsActions from 'src/types/actions/notifications'
import {setBucketInfo} from 'src/dataLoaders/actions/steps'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Telegraf, Bucket} from '@influxdata/influx'
import {OverlayState} from 'src/types'
import {
  setDataLoadersType,
  setTelegrafConfigID,
  setTelegrafConfigName,
  clearDataLoaders,
} from 'src/dataLoaders/actions/dataLoaders'
import {DataLoaderType} from 'src/types/v2/dataLoaders'

interface OwnProps {
  collectors: Telegraf[]
  onChange: () => void
  notify: NotificationsActions.PublishNotificationActionCreator
  orgName: string
  buckets: Bucket[]
}

interface DispatchProps {
  onSetBucketInfo: typeof setBucketInfo
  onSetDataLoadersType: typeof setDataLoadersType
  onSetTelegrafConfigID: typeof setTelegrafConfigID
  onSetTelegrafConfigName: typeof setTelegrafConfigName
  onClearDataLoaders: typeof clearDataLoaders
}

type Props = OwnProps & DispatchProps

interface State {
  dataLoaderOverlay: OverlayState
  searchTerm: string
  instructionsOverlay: OverlayState
  collectorID?: string
  telegrafConfig: OverlayState
}

@ErrorHandling
export class Collectors extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      dataLoaderOverlay: OverlayState.Closed,
      searchTerm: '',
      instructionsOverlay: OverlayState.Closed,
      collectorID: null,
      telegrafConfig: OverlayState.Closed,
    }
  }

  public render() {
    const {collectors, buckets} = this.props
    const {searchTerm} = this.state

    return (
      <>
        <Tabs.TabContentsHeader>
          <Input
            icon={IconFont.Search}
            placeholder="Filter telegraf configs by bucket..."
            widthPixels={290}
            value={searchTerm}
            type={InputType.Text}
            onChange={this.handleFilterChange}
            onBlur={this.handleFilterBlur}
          />
          {this.createButton}
        </Tabs.TabContentsHeader>
        <Grid>
          <Grid.Row>
            <Grid.Column widthSM={Columns.Twelve}>
              <FilterList<Telegraf>
                searchTerm={searchTerm}
                searchKeys={['plugins.0.config.bucket', 'labels[].name']}
                list={collectors}
              >
                {cs => (
                  <CollectorList
                    collectors={cs}
                    emptyState={this.emptyState}
                    onDelete={this.handleDeleteTelegraf}
                    onUpdate={this.handleUpdateTelegraf}
                    onOpenInstructions={this.handleOpenInstructions}
                    onOpenTelegrafConfig={this.handleOpenTelegrafConfig}
                    onFilterChange={this.handleFilterUpdate}
                  />
                )}
              </FilterList>
            </Grid.Column>
            <Grid.Column
              widthSM={Columns.Six}
              widthMD={Columns.Four}
              offsetSM={Columns.Three}
              offsetMD={Columns.Four}
            >
              <TelegrafExplainer />
            </Grid.Column>
          </Grid.Row>
        </Grid>
        <CollectorsWizard
          visible={this.isDataLoaderVisible}
          onCompleteSetup={this.handleDismissDataLoaders}
          startingStep={0}
          buckets={buckets}
        />
        <TelegrafInstructionsOverlay
          visible={this.isInstructionsVisible}
          collector={this.selectedCollector}
          onDismiss={this.handleCloseInstructions}
        />
        <TelegrafConfigOverlay
          visible={this.isTelegrafConfigVisible}
          onDismiss={this.handleCloseTelegrafConfig}
        />
      </>
    )
  }

  private get selectedCollector() {
    return this.props.collectors.find(c => c.id === this.state.collectorID)
  }

  private get isDataLoaderVisible(): boolean {
    return this.state.dataLoaderOverlay === OverlayState.Open
  }

  private get isInstructionsVisible(): boolean {
    return this.state.instructionsOverlay === OverlayState.Open
  }

  private handleOpenInstructions = (collectorID: string): void => {
    this.setState({
      instructionsOverlay: OverlayState.Open,
      collectorID,
    })
  }

  private handleCloseInstructions = (): void => {
    this.setState({
      instructionsOverlay: OverlayState.Closed,
      collectorID: null,
    })
  }

  private get isTelegrafConfigVisible(): boolean {
    return this.state.telegrafConfig === OverlayState.Open
  }

  private handleOpenTelegrafConfig = (
    telegrafID: string,
    telegrafName: string
  ): void => {
    this.props.onSetTelegrafConfigID(telegrafID)
    this.props.onSetTelegrafConfigName(telegrafName)
    this.setState({
      telegrafConfig: OverlayState.Open,
    })
  }

  private handleCloseTelegrafConfig = (): void => {
    this.props.onClearDataLoaders()
    this.setState({
      telegrafConfig: OverlayState.Closed,
    })
  }

  private get createButton(): JSX.Element {
    return (
      <Button
        text="Create Configuration"
        icon={IconFont.Plus}
        color={ComponentColor.Primary}
        onClick={this.handleAddCollector}
      />
    )
  }

  private handleAddCollector = () => {
    const {buckets, onSetBucketInfo, onSetDataLoadersType} = this.props

    if (buckets && buckets.length) {
      const {organization, organizationID, name, id} = buckets[0]
      onSetBucketInfo(organization, organizationID, name, id)
    }

    onSetDataLoadersType(DataLoaderType.Scraping)

    this.setState({dataLoaderOverlay: OverlayState.Open})
  }

  private handleDismissDataLoaders = () => {
    this.setState({dataLoaderOverlay: OverlayState.Closed})
    this.props.onChange()
  }

  private get emptyState(): JSX.Element {
    const {orgName} = this.props
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text
            text={`${orgName} does not own any Telegraf  Configurations, why not create one?`}
            highlightWords={['Telegraf', 'Configurations']}
          />
          {this.createButton}
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text text="No Telegraf  Configuration buckets match your query" />
      </EmptyState>
    )
  }

  private handleDeleteTelegraf = async (telegrafID: string) => {
    await client.telegrafConfigs.delete(telegrafID)
    this.props.onChange()
  }

  private handleUpdateTelegraf = async (telegraf: Telegraf) => {
    await client.telegrafConfigs.update(telegraf.id, telegraf)
    this.props.onChange()
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.handleFilterUpdate(e.target.value)
  }

  private handleFilterBlur = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleFilterUpdate = (searchTerm: string) => {
    this.setState({searchTerm})
  }
}

const mdtp: DispatchProps = {
  onSetBucketInfo: setBucketInfo,
  onSetDataLoadersType: setDataLoadersType,
  onSetTelegrafConfigID: setTelegrafConfigID,
  onSetTelegrafConfigName: setTelegrafConfigName,
  onClearDataLoaders: clearDataLoaders,
}

export default connect<null, DispatchProps, OwnProps>(
  null,
  mdtp
)(Collectors)
