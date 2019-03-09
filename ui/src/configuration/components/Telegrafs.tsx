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

// Actions
import {deleteLabel} from 'src/labels/actions'
import {setBucketInfo} from 'src/dataLoaders/actions/steps'
import {
  setDataLoadersType,
  setTelegrafConfigID,
  setTelegrafConfigName,
  clearDataLoaders,
} from 'src/dataLoaders/actions/dataLoaders'
import {
  updateTelegraf,
  createTelegraf,
  deleteTelegraf,
} from 'src/telegrafs/actions'
import {deleteAuthorization} from 'src/authorizations/actions'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Telegraf, Bucket, Organization} from '@influxdata/influx'
import {OverlayState} from 'src/types'
import {DataLoaderType} from 'src/types/v2/dataLoaders'
import {AppState} from 'src/types/v2'

interface StateProps {
  org: Organization
  buckets: Bucket[]
  telegrafs: Telegraf[]
}

interface DispatchProps {
  onSetBucketInfo: typeof setBucketInfo
  onSetDataLoadersType: typeof setDataLoadersType
  onSetTelegrafConfigID: typeof setTelegrafConfigID
  onSetTelegrafConfigName: typeof setTelegrafConfigName
  onClearDataLoaders: typeof clearDataLoaders
  updateTelegraf: typeof updateTelegraf
  deleteTelegraf: typeof deleteTelegraf
  createTelegraf: typeof createTelegraf
  deleteLabel: typeof deleteLabel
  deleteAuthorization: typeof deleteAuthorization
}

type Props = StateProps & DispatchProps

interface State {
  dataLoaderOverlay: OverlayState
  searchTerm: string
  instructionsOverlay: OverlayState
  collectorID?: string
  telegrafConfig: OverlayState
}

@ErrorHandling
export class Telegrafs extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchTerm: '',
      collectorID: null,
      dataLoaderOverlay: OverlayState.Closed,
      instructionsOverlay: OverlayState.Closed,
      telegrafConfig: OverlayState.Closed,
    }
  }

  public render() {
    const {telegrafs} = this.props
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
              <FilterList<Telegraf>
                searchTerm={searchTerm}
                searchKeys={['plugins.0.config.bucket', 'labels[].name']}
                list={telegrafs}
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
        {this.telegrafsWizard}
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

  private get telegrafsWizard(): JSX.Element {
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
    return this.props.telegrafs.find(c => c.id === this.state.collectorID)
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

    onSetDataLoadersType(DataLoaderType.Streaming)

    this.setState({dataLoaderOverlay: OverlayState.Open})
  }

  private handleDismissDataLoaders = () => {
    this.setState({dataLoaderOverlay: OverlayState.Closed})
  }

  private get emptyState(): JSX.Element {
    const {org} = this.props
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text
            text={`${
              org.name
            } does not own any Telegraf  Configurations, why not create one?`}
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
    this.props.deleteTelegraf(telegraf.id, telegraf.name)

    // hack to remove stale tokens from system when telegraf is deleted
    const label = telegraf.labels.find(l => l.name == 'token')

    if (label) {
      this.props.deleteLabel(label.id)
      this.props.deleteAuthorization(label.properties.tokenID)
    }
  }

  private handleUpdateTelegraf = async (telegraf: Telegraf) => {
    this.props.updateTelegraf(telegraf)
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
  updateTelegraf,
  createTelegraf,
  deleteTelegraf,
  deleteLabel,
  deleteAuthorization,
}

const mstp = ({orgs, buckets, telegrafs}: AppState): StateProps => {
  const org = orgs[0]
  return {
    org,
    buckets: buckets.list,
    telegrafs: telegrafs.list,
  }
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(Telegrafs)
