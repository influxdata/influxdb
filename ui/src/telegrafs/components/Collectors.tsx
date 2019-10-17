// Libraries
import _ from 'lodash'
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Button, EmptyState, Grid, Sort} from '@influxdata/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import SettingsTabbedPageHeader from 'src/settings/components/SettingsTabbedPageHeader'
import CollectorList from 'src/telegrafs/components/CollectorList'
import TelegrafExplainer from 'src/telegrafs/components/TelegrafExplainer'
import FilterList from 'src/shared/components/Filter'
import NoBucketsWarning from 'src/buckets/components/NoBucketsWarning'
import GetResources, {ResourceType} from 'src/shared/components/GetResources'

// Actions
import {setBucketInfo} from 'src/dataLoaders/actions/steps'
import {updateTelegraf, deleteTelegraf} from 'src/telegrafs/actions'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {ITelegraf as Telegraf} from '@influxdata/influx'
import {
  Columns,
  IconFont,
  ComponentSize,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'
import {OverlayState, AppState, Bucket} from 'src/types'
import {
  setDataLoadersType,
  setTelegrafConfigID,
  setTelegrafConfigName,
  clearDataLoaders,
} from 'src/dataLoaders/actions/dataLoaders'
import {DataLoaderType} from 'src/types/dataLoaders'
import {SortTypes} from 'src/shared/utils/sort'

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
  sortKey: SortKey
  sortDirection: Sort
  sortType: SortTypes
}

type SortKey = keyof Telegraf

@ErrorHandling
class Collectors extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      dataLoaderOverlay: OverlayState.Closed,
      searchTerm: '',
      instructionsOverlay: OverlayState.Closed,
      collectorID: null,
      sortKey: 'name',
      sortDirection: Sort.Ascending,
      sortType: SortTypes.String,
    }
  }

  public render() {
    const {collectors} = this.props
    const {searchTerm, sortKey, sortDirection, sortType} = this.state

    return (
      <>
        <NoBucketsWarning
          visible={this.hasNoBuckets}
          resourceName="Telegraf Configurations"
        />

        <SettingsTabbedPageHeader>
          <SearchWidget
            placeholderText="Filter telegraf configurations..."
            searchTerm={searchTerm}
            onSearch={this.handleFilterChange}
          />
          {this.createButton}
        </SettingsTabbedPageHeader>
        <Grid>
          <Grid.Row>
            <Grid.Column
              widthXS={Columns.Twelve}
              widthSM={Columns.Eight}
              widthMD={Columns.Ten}
            >
              <GetResources resource={ResourceType.Labels}>
                <FilterList<Telegraf>
                  searchTerm={searchTerm}
                  searchKeys={['plugins.0.config.bucket', 'name']}
                  list={collectors}
                >
                  {cs => (
                    <CollectorList
                      collectors={cs}
                      emptyState={this.emptyState}
                      onDelete={this.handleDeleteTelegraf}
                      onUpdate={this.handleUpdateTelegraf}
                      onFilterChange={this.handleFilterUpdate}
                      sortKey={sortKey}
                      sortDirection={sortDirection}
                      sortType={sortType}
                      onClickColumn={this.handleClickColumn}
                    />
                  )}
                </FilterList>
              </GetResources>
            </Grid.Column>
            <Grid.Column
              widthXS={Columns.Twelve}
              widthSM={Columns.Four}
              widthMD={Columns.Two}
            >
              <TelegrafExplainer />
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </>
    )
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    const sortType = SortTypes.String
    this.setState({sortKey, sortDirection: nextSort, sortType})
  }

  private get hasNoBuckets(): boolean {
    const {buckets} = this.props

    if (!buckets || !buckets.length) {
      return true
    }

    return false
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
      const {orgID, name, id} = buckets[0]
      onSetBucketInfo(orgID, name, id)
    }

    onSetDataLoadersType(DataLoaderType.Scraping)

    router.push(`/orgs/${orgID}/load-data/telegrafs/new`)
  }

  private get emptyState(): JSX.Element {
    const {orgName} = this.props
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text>
            {`${orgName}`} does not own any <b>Telegraf Configurations</b>, why
            not create one?
          </EmptyState.Text>
          {this.createButton}
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text>
          No <b>Telegraf Configurations</b> match your query
        </EmptyState.Text>
      </EmptyState>
    )
  }

  private handleDeleteTelegraf = (telegraf: Telegraf) => {
    this.props.onDeleteTelegraf(telegraf.id, telegraf.name)
  }

  private handleUpdateTelegraf = (telegraf: Telegraf) => {
    this.props.onUpdateTelegraf(telegraf)
  }

  private handleFilterChange = (searchTerm: string): void => {
    this.handleFilterUpdate(searchTerm)
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
