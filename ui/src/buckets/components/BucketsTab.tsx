// Libraries
import React, {PureComponent} from 'react'
import {isEmpty} from 'lodash'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  Grid,
  IconFont,
  ComponentSize,
  ComponentColor,
  Sort,
  Button,
  EmptyState,
  ComponentStatus,
  Columns,
  Overlay,
} from '@influxdata/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import SettingsTabbedPageHeader from 'src/settings/components/SettingsTabbedPageHeader'
import FilterList from 'src/shared/components/Filter'
import BucketList from 'src/buckets/components/BucketList'
import {PrettyBucket} from 'src/buckets/components/BucketCard'
import CreateBucketOverlay from 'src/buckets/components/CreateBucketOverlay'
import AssetLimitAlert from 'src/cloud/components/AssetLimitAlert'
import BucketExplainer from 'src/buckets/components/BucketExplainer'

// Actions
import {createBucket, updateBucket, deleteBucket} from 'src/buckets/actions'
import {
  checkBucketLimits as checkBucketLimitsAction,
  LimitStatus,
} from 'src/cloud/actions/limits'

// Utils
import {prettyBuckets} from 'src/shared/utils/prettyBucket'
import {extractBucketLimits} from 'src/cloud/utils/limits'
import {getOrg} from 'src/organizations/selectors'

// Types
import {OverlayState, AppState, Bucket, Organization} from 'src/types'
import {SortTypes} from 'src/shared/utils/sort'

interface StateProps {
  org: Organization
  buckets: Bucket[]
  limitStatus: LimitStatus
}

interface DispatchProps {
  createBucket: typeof createBucket
  updateBucket: typeof updateBucket
  deleteBucket: typeof deleteBucket
  checkBucketLimits: typeof checkBucketLimitsAction
}

interface State {
  searchTerm: string
  overlayState: OverlayState
  sortKey: SortKey
  sortDirection: Sort
  sortType: SortTypes
}

type Props = DispatchProps & StateProps

type SortKey = keyof PrettyBucket

@ErrorHandling
class BucketsTab extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchTerm: '',
      overlayState: OverlayState.Closed,
      sortKey: 'name',
      sortDirection: Sort.Ascending,
      sortType: SortTypes.String,
    }
  }

  public componentDidMount() {
    this.props.checkBucketLimits()
  }

  public render() {
    const {org, buckets, limitStatus} = this.props
    const {
      searchTerm,
      overlayState,
      sortKey,
      sortDirection,
      sortType,
    } = this.state

    return (
      <>
        <AssetLimitAlert
          resourceName="buckets"
          limitStatus={limitStatus}
          className="load-data--asset-alert"
        />
        <SettingsTabbedPageHeader>
          <SearchWidget
            placeholderText="Filter buckets..."
            searchTerm={searchTerm}
            onSearch={this.handleFilterChange}
          />
          <Button
            text="Create Bucket"
            icon={IconFont.Plus}
            color={ComponentColor.Primary}
            onClick={this.handleOpenModal}
            testID="Create Bucket"
            status={this.createButtonStatus}
            titleText={this.createButtonTitleText}
          />
        </SettingsTabbedPageHeader>
        <Grid>
          <Grid.Row>
            <Grid.Column
              widthXS={Columns.Twelve}
              widthSM={Columns.Eight}
              widthMD={Columns.Ten}
            >
              <FilterList<PrettyBucket>
                searchTerm={searchTerm}
                searchKeys={['name', 'ruleString', 'labels[].name']}
                list={prettyBuckets(buckets)}
              >
                {bs => (
                  <BucketList
                    buckets={bs}
                    emptyState={this.emptyState}
                    onUpdateBucket={this.handleUpdateBucket}
                    onDeleteBucket={this.handleDeleteBucket}
                    onFilterChange={this.handleFilterUpdate}
                    sortKey={sortKey}
                    sortDirection={sortDirection}
                    sortType={sortType}
                    onClickColumn={this.handleClickColumn}
                  />
                )}
              </FilterList>
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
        <Overlay visible={overlayState === OverlayState.Open}>
          <CreateBucketOverlay
            org={org}
            onCloseModal={this.handleCloseModal}
            onCreateBucket={this.handleCreateBucket}
          />
        </Overlay>
      </>
    )
  }

  private handleClickColumn = (sortType: SortTypes) => (
    nextSort: Sort,
    sortKey: SortKey
  ) => {
    this.setState({sortKey, sortDirection: nextSort, sortType})
  }

  private handleUpdateBucket = (updatedBucket: PrettyBucket) => {
    this.props.updateBucket(updatedBucket as Bucket)
  }

  private handleDeleteBucket = ({id, name}: PrettyBucket) => {
    this.props.deleteBucket(id, name)
  }

  private handleCreateBucket = (bucket: Bucket) => {
    this.props.createBucket(bucket)
    this.handleCloseModal()
  }

  private handleOpenModal = (): void => {
    this.setState({overlayState: OverlayState.Open})
  }

  private handleCloseModal = (): void => {
    this.setState({overlayState: OverlayState.Closed})
  }

  private handleFilterChange = (searchTerm: string): void => {
    this.handleFilterUpdate(searchTerm)
  }

  private handleFilterUpdate = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private get createButtonStatus(): ComponentStatus {
    if (this.props.limitStatus === LimitStatus.EXCEEDED) {
      return ComponentStatus.Disabled
    }
    return ComponentStatus.Default
  }

  private get createButtonTitleText(): string {
    if (this.props.limitStatus === LimitStatus.EXCEEDED) {
      return 'This account has the maximum number of buckets allowed'
    }
    return 'Create a bucket'
  }

  private get emptyState(): JSX.Element {
    const {searchTerm} = this.state

    if (isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text>
            Looks like there aren't any <b>Buckets</b>, why not create one?
          </EmptyState.Text>
          <Button
            text="Create Bucket"
            icon={IconFont.Plus}
            color={ComponentColor.Primary}
            onClick={this.handleOpenModal}
          />
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

const mstp = (state: AppState): StateProps => ({
  buckets: state.buckets.list,
  org: getOrg(state),
  limitStatus: extractBucketLimits(state.cloud.limits),
})

const mdtp = {
  createBucket,
  updateBucket,
  deleteBucket,
  checkBucketLimits: checkBucketLimitsAction,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(BucketsTab)
