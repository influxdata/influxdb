// Libraries
import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Input, Button, EmptyState, Grid} from '@influxdata/clockface'
import {Tabs} from 'src/clockface'
import CollectorList from 'src/telegrafs/components/CollectorList'
import TelegrafExplainer from 'src/telegrafs/components/TelegrafExplainer'
import FilterList from 'src/shared/components/Filter'
import NoBucketsWarning from 'src/buckets/components/NoBucketsWarning'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

// Actions
import {setBucketInfo} from 'src/dataLoaders/actions/steps'
import {updateTelegraf, deleteTelegraf} from 'src/telegrafs/actions'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {ITelegraf as Telegraf, Bucket} from '@influxdata/influx'
import {
  Columns,
  IconFont,
  InputType,
  ComponentSize,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'
import {OverlayState, AppState} from 'src/types'
import {
  setDataLoadersType,
  setTelegrafConfigID,
  setTelegrafConfigName,
  clearDataLoaders,
} from 'src/dataLoaders/actions/dataLoaders'
import {DataLoaderType} from 'src/types/dataLoaders'

interface StateProps {
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

type Props = DispatchProps & StateProps & WithRouterProps

interface State {
  dataLoaderOverlay: OverlayState
  searchTerm: string
  instructionsOverlay: OverlayState
  collectorID?: string
}

@ErrorHandling
class Collectors extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      dataLoaderOverlay: OverlayState.Closed,
      searchTerm: '',
      instructionsOverlay: OverlayState.Closed,
      collectorID: null,
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
              <GetResources resource={ResourceTypes.Labels}>
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
                      onFilterChange={this.handleFilterUpdate}
                    />
                  )}
                </FilterList>
              </GetResources>
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

  private handleOpenInstructions = (collectorID: string): void => {
    const {
      router,
      params: {orgID},
    } = this.props
    this.setState({
      collectorID,
    })

    router.push(`/orgs/${orgID}/telegrafs/${collectorID}/instructions`)
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
    const {
      buckets,
      onSetBucketInfo,
      onSetDataLoadersType,
      router,
      params: {orgID},
    } = this.props

    if (buckets && buckets.length) {
      const {organization, organizationID, name, id} = buckets[0]
      onSetBucketInfo(organization, organizationID, name, id)
    }

    onSetDataLoadersType(DataLoaderType.Scraping)

    router.push(`/orgs/${orgID}/telegrafs/new`)
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
const mstp = ({telegrafs, orgs: {org}, buckets}: AppState): StateProps => ({
  collectors: telegrafs.list,
  orgName: org.name,
  buckets: buckets.list,
})

const mdtp: DispatchProps = {
  onSetBucketInfo: setBucketInfo,
  onSetDataLoadersType: setDataLoadersType,
  onSetTelegrafConfigID: setTelegrafConfigID,
  onSetTelegrafConfigName: setTelegrafConfigName,
  onClearDataLoaders: clearDataLoaders,
  onUpdateTelegraf: updateTelegraf,
  onDeleteTelegraf: deleteTelegraf,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter<StateProps & DispatchProps>(Collectors))
