// Libraries
import React, {PureComponent} from 'react'
import {isEmpty} from 'lodash'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  Grid,
  ComponentSize,
  Sort,
  EmptyState,
  Columns,
} from '@influxdata/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import FilterList from 'src/shared/components/FilterList'
import BucketList from 'src/buckets/components/BucketList'
import AssetLimitAlert from 'src/cloud/components/AssetLimitAlert'
import BucketExplainer from 'src/buckets/components/BucketExplainer'
import DemoDataDropdown from 'src/buckets/components/DemoDataDropdown'
import {FeatureFlag} from 'src/shared/utils/featureFlag'
import ResourceSortDropdown from 'src/shared/components/resource_sort_dropdown/ResourceSortDropdown'
import CreateBucketButton from 'src/buckets/components/CreateBucketButton'

// Actions
import {
  createBucket,
  updateBucket,
  deleteBucket,
} from 'src/buckets/actions/thunks'
import {
  checkBucketLimits as checkBucketLimitsAction,
  LimitStatus,
} from 'src/cloud/actions/limits'
import {
  getDemoDataBuckets as getDemoDataBucketsAction,
  getDemoDataBucketMembership as getDemoDataBucketMembershipAction,
} from 'src/cloud/actions/demodata'

// Utils
import {getNewDemoBuckets} from 'src/cloud/selectors/demodata'
import {extractBucketLimits} from 'src/cloud/utils/limits'
import {getAll} from 'src/resources/selectors'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'
import {SortTypes} from 'src/shared/utils/sort'

// Types
import {AppState, Bucket, ResourceType, OwnBucket} from 'src/types'
import {BucketSortKey} from 'src/shared/components/resource_sort_dropdown/generateSortItems'

interface StateProps {
  buckets: Bucket[]
  limitStatus: LimitStatus
  demoDataBuckets: Bucket[]
}

interface DispatchProps {
  createBucket: typeof createBucket
  updateBucket: typeof updateBucket
  deleteBucket: typeof deleteBucket
  checkBucketLimits: typeof checkBucketLimitsAction
  getDemoDataBuckets: typeof getDemoDataBucketsAction
  getDemoDataBucketMembership: typeof getDemoDataBucketMembershipAction
}

interface State {
  searchTerm: string
  sortKey: BucketSortKey
  sortDirection: Sort
  sortType: SortTypes
}

type Props = DispatchProps & StateProps

const FilterBuckets = FilterList<Bucket>()

@ErrorHandling
class BucketsTab extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchTerm: '',
      sortKey: 'name',
      sortDirection: Sort.Ascending,
      sortType: SortTypes.String,
    }
  }

  public componentDidMount() {
    this.props.checkBucketLimits()
    if (isFlagEnabled('demodata')) {
      this.props.getDemoDataBuckets()
    }
  }

  public render() {
    const {
      buckets,
      limitStatus,
      demoDataBuckets,
      getDemoDataBucketMembership,
    } = this.props
    const {searchTerm, sortKey, sortDirection, sortType} = this.state

    const leftHeaderItems = (
      <>
        <SearchWidget
          placeholderText="Filter buckets..."
          searchTerm={searchTerm}
          onSearch={this.handleFilterUpdate}
        />
        <ResourceSortDropdown
          resourceType={ResourceType.Buckets}
          sortDirection={sortDirection}
          sortKey={sortKey}
          sortType={sortType}
          onSelect={this.handleSort}
          width={238}
        />
      </>
    )

    const rightHeaderItems = (
      <>
        <FeatureFlag name="demodata">
          {demoDataBuckets.length > 0 && (
            <DemoDataDropdown
              buckets={demoDataBuckets}
              getMembership={getDemoDataBucketMembership}
            />
          )}
        </FeatureFlag>
        <CreateBucketButton />
      </>
    )

    return (
      <>
        <AssetLimitAlert
          resourceName="buckets"
          limitStatus={limitStatus}
          className="load-data--asset-alert"
        />
        <TabbedPageHeader
          childrenLeft={leftHeaderItems}
          childrenRight={rightHeaderItems}
        />
        <Grid>
          <Grid.Row>
            <Grid.Column
              widthXS={Columns.Twelve}
              widthSM={Columns.Eight}
              widthMD={Columns.Ten}
            >
              <FilterBuckets
                searchTerm={searchTerm}
                searchKeys={['name', 'readableRetention', 'labels[].name']}
                list={buckets}
              >
                {bs => (
                  <BucketList
                    buckets={bs}
                    emptyState={this.emptyState}
                    onUpdateBucket={this.props.updateBucket}
                    onDeleteBucket={this.handleDeleteBucket}
                    onFilterChange={this.handleFilterUpdate}
                    sortKey={sortKey}
                    sortDirection={sortDirection}
                    sortType={sortType}
                  />
                )}
              </FilterBuckets>
            </Grid.Column>
            <Grid.Column
              widthXS={Columns.Twelve}
              widthSM={Columns.Four}
              widthMD={Columns.Two}
            >
              <BucketExplainer />
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </>
    )
  }

  private handleSort = (
    sortKey: BucketSortKey,
    sortDirection: Sort,
    sortType: SortTypes
  ): void => {
    this.setState({sortKey, sortDirection, sortType})
  }

  private handleDeleteBucket = ({id, name}: OwnBucket) => {
    this.props.deleteBucket(id, name)
  }

  private handleFilterUpdate = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private get emptyState(): JSX.Element {
    const {searchTerm} = this.state

    if (isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text>
            Looks like there aren't any <b>Buckets</b>, why not create one?
          </EmptyState.Text>
          <CreateBucketButton />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text>No Buckets match your query</EmptyState.Text>
      </EmptyState>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const buckets = getAll<Bucket>(state, ResourceType.Buckets)
  return {
    buckets,
    limitStatus: extractBucketLimits(state.cloud.limits),
    demoDataBuckets: getNewDemoBuckets(state, buckets),
  }
}

const mdtp: DispatchProps = {
  createBucket,
  updateBucket,
  deleteBucket,
  checkBucketLimits: checkBucketLimitsAction,
  getDemoDataBuckets: getDemoDataBucketsAction,
  getDemoDataBucketMembership: getDemoDataBucketMembershipAction,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(BucketsTab)
