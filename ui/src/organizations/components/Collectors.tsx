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
  ComponentStatus,
} from '@influxdata/clockface'
import {EmptyState, Grid, Input, InputType, Tabs} from 'src/clockface'
import CollectorsWizard from 'src/dataLoaders/components/collectorsWizard/CollectorsWizard'
import FilterList from 'src/shared/components/Filter'
import NoBucketsWarning from 'src/organizations/components/NoBucketsWarning'
import GetLabels from 'src/configuration/components/GetLabels'

// Actions
import {setBucketInfo} from 'src/dataLoaders/actions/steps'
import {updateTelegraf, deleteTelegraf} from 'src/telegrafs/actions'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {ITelegraf as Telegraf, Bucket} from '@influxdata/influx'
import {OverlayState} from 'src/types'
import {
  setDataLoadersType,
  setTelegrafConfigID,
  setTelegrafConfigName,
  clearDataLoaders,
} from 'src/dataLoaders/actions/dataLoaders'
import {DataLoaderType} from 'src/types/dataLoaders'

interface OwnProps {
  collectors: Telegraf[]
  orgName: string
  buckets: Bucket[]
}

interface DispatchProps {
  onSetBucketInfo: typeof setBucketInfo
  onSetDataLoadersType: typeof setDataLoadersType
  onSetTelegrafConfigID: typeof setTelegrafConfigID
  onSetTelegrafConfigName: typeof setTelegrafConfigName
  onClearDataLoaders: typeof clearDataLoaders
  onUpdateTelegraf: typeof updateTelegraf
  onDeleteTelegraf: typeof deleteTelegraf
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
    const {collectors} = this.props
    const {searchTerm} = this.state

    return (
      <>
        <Tabs.TabContentsHeader>
          <Input
            icon={IconFont.Search}
            placeholder="Filter telegraf configs..."
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
              <NoBucketsWarning
                visible={this.hasNoBuckets}
                resourceName="Telegraf Configurations"
              />
              <GetLabels>
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
              </GetLabels>
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
        {this.collectorsWizard}
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

  private get hasNoBuckets(): boolean {
    const {buckets} = this.props

    if (!buckets || !buckets.length) {
      return true
    }

    return false
  }

  private get collectorsWizard(): JSX.Element {
    const {buckets} = this.props

    if (this.hasNoBuckets) {
      return
    }

    return (
      <CollectorsWizard
        visible={this.isDataLoaderVisible}
        onCompleteSetup={this.handleDismissDataLoaders}
        startingStep={0}
        buckets={buckets}
      />
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
    let status = ComponentStatus.Default
    let titleText = 'Create a new Telegraf Configuration'

    if (this.hasNoBuckets) {
      status = ComponentStatus.Disabled
      titleText =
        'You need at least 1 bucket in order to create a Telegraf Configuration'
    }

    return (
      <Button
        text="Create Configuration"
        icon={IconFont.Plus}
        color={ComponentColor.Primary}
        onClick={this.handleAddCollector}
        status={status}
        titleText={titleText}
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

  private handleDeleteTelegraf = async (telegraf: Telegraf) => {
    await this.props.onDeleteTelegraf(telegraf.id, telegraf.name)
  }

  private handleUpdateTelegraf = async (telegraf: Telegraf) => {
    await this.props.onUpdateTelegraf(telegraf)
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
  onUpdateTelegraf: updateTelegraf,
  onDeleteTelegraf: deleteTelegraf,
}

export default connect<null, DispatchProps, OwnProps>(
  null,
  mdtp
)(Collectors)
