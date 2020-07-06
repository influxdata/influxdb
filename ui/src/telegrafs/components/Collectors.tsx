// Libraries
import _ from 'lodash'
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import {
  Button,
  EmptyState,
  Grid,
  Sort,
  Columns,
  IconFont,
  ComponentSize,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import {FilteredList} from 'src/telegrafs/components/CollectorList'
import TelegrafExplainer from 'src/telegrafs/components/TelegrafExplainer'
import NoBucketsWarning from 'src/buckets/components/NoBucketsWarning'
import GetResources from 'src/resources/components/GetResources'
import ResourceSortDropdown from 'src/shared/components/resource_sort_dropdown/ResourceSortDropdown'

// Actions
import {updateTelegraf, deleteTelegraf} from 'src/telegrafs/actions/thunks'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {OverlayState, AppState, Bucket, ResourceType} from 'src/types'
import {
  setTelegrafConfigID,
  setTelegrafConfigName,
  clearDataLoaders,
} from 'src/dataLoaders/actions/dataLoaders'
import {SortTypes} from 'src/shared/utils/sort'
import {TelegrafSortKey} from 'src/shared/components/resource_sort_dropdown/generateSortItems'

// Selectors
import {getOrg} from 'src/organizations/selectors'
import {getAll} from 'src/resources/selectors'

interface StateProps {
  hasTelegrafs: boolean
  orgName: string
  buckets: Bucket[]
}

interface DispatchProps {
  onSetTelegrafConfigID: typeof setTelegrafConfigID
  onSetTelegrafConfigName: typeof setTelegrafConfigName
  onClearDataLoaders: typeof clearDataLoaders
  onUpdateTelegraf: typeof updateTelegraf
  onDeleteTelegraf: typeof deleteTelegraf
}

type Props = DispatchProps & StateProps & RouteComponentProps<{orgID: string}>

interface State {
  dataLoaderOverlay: OverlayState
  searchTerm: string
  instructionsOverlay: OverlayState
  collectorID?: string
  sortKey: TelegrafSortKey
  sortDirection: Sort
  sortType: SortTypes
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
      sortKey: 'name',
      sortDirection: Sort.Ascending,
      sortType: SortTypes.String,
    }
  }

  public render() {
    const {hasTelegrafs} = this.props
    const {searchTerm, sortKey, sortDirection, sortType} = this.state

    const collecorsLeftHeaderItems = (
      <>
        <SearchWidget
          placeholderText="Filter telegraf configurations..."
          searchTerm={searchTerm}
          onSearch={this.handleFilterChange}
        />
        <ResourceSortDropdown
          resourceType={ResourceType.Telegrafs}
          sortDirection={sortDirection}
          sortKey={sortKey}
          sortType={sortType}
          onSelect={this.handleSort}
        />
      </>
    )

    const collecorsRightHeaderItems = (
      <>
        <Button
          text="InfluxDB Output Plugin"
          icon={IconFont.Eye}
          color={ComponentColor.Secondary}
          onClick={this.handleJustTheOutput}
          titleText="Output section of telegraf.conf for V2"
          testID="button--output-only"
        />
        {this.createButton}
      </>
    )

    return (
      <>
        <NoBucketsWarning
          visible={this.hasNoBuckets}
          resourceName="Telegraf Configurations"
        />
        <TabbedPageHeader
          childrenLeft={collecorsLeftHeaderItems}
          childrenRight={collecorsRightHeaderItems}
        />
        <Grid>
          <Grid.Row>
            <Grid.Column
              widthXS={Columns.Twelve}
              widthSM={hasTelegrafs ? Columns.Eight : Columns.Twelve}
              widthMD={hasTelegrafs ? Columns.Ten : Columns.Twelve}
            >
              <GetResources resources={[ResourceType.Labels]}>
                <FilteredList
                  searchTerm={searchTerm}
                  emptyState={this.emptyState}
                  onFilterChange={this.handleFilterUpdate}
                  sortKey={sortKey}
                  sortDirection={sortDirection}
                  sortType={sortType}
                />
              </GetResources>
            </Grid.Column>
            {hasTelegrafs && (
              <Grid.Column
                widthXS={Columns.Twelve}
                widthSM={Columns.Four}
                widthMD={Columns.Two}
              >
                <TelegrafExplainer />
              </Grid.Column>
            )}
          </Grid.Row>
        </Grid>
      </>
    )
  }

  private handleSort = (
    sortKey: TelegrafSortKey,
    sortDirection: Sort,
    sortType: SortTypes
  ): void => {
    this.setState({sortKey, sortDirection, sortType})
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
      history,
      match: {
        params: {orgID},
      },
    } = this.props

    history.push(`/orgs/${orgID}/load-data/telegrafs/new`)
  }

  private handleJustTheOutput = () => {
    const {
      history,
      match: {
        params: {orgID},
      },
    } = this.props

    history.push(`/orgs/${orgID}/load-data/telegrafs/output`)
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
          <br />
          <br />
          <TelegrafExplainer
            hasNoTelegrafs={true}
            textAlign="center"
            bodySize={ComponentSize.Medium}
          />
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

  private handleFilterChange = (searchTerm: string): void => {
    this.handleFilterUpdate(searchTerm)
  }

  private handleFilterUpdate = (searchTerm: string) => {
    this.setState({searchTerm})
  }
}
const mstp = (state: AppState): StateProps => {
  const {telegrafs} = state.resources
  const orgName = getOrg(state).name
  const buckets = getAll<Bucket>(state, ResourceType.Buckets)
  const hasTelegrafs = !!telegrafs.allIDs.length

  return {
    hasTelegrafs,
    orgName,
    buckets,
  }
}

const mdtp: DispatchProps = {
  onSetTelegrafConfigID: setTelegrafConfigID,
  onSetTelegrafConfigName: setTelegrafConfigName,
  onClearDataLoaders: clearDataLoaders,
  onUpdateTelegraf: updateTelegraf,
  onDeleteTelegraf: deleteTelegraf,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter(Collectors))
