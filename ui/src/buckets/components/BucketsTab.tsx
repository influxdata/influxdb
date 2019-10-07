// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  ComponentSize,
  Sort,
  EmptyState,
} from '@influxdata/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import SettingsTabbedPageHeader from 'src/settings/components/SettingsTabbedPageHeader'
import FilterList from 'src/shared/components/Filter'
import BucketList from 'src/buckets/components/BucketList'
import CreateBucketButton from 'src/buckets/components/CreateBucketButton'
import {PrettyBucket} from 'src/buckets/components/BucketCard'
import AssetLimitAlert from 'src/cloud/components/AssetLimitAlert'

// Actions
import {updateBucket, deleteBucket} from 'src/buckets/actions'
import {
  checkBucketLimits as checkBucketLimitsAction,
  LimitStatus,
} from 'src/cloud/actions/limits'

// Utils
import {prettyBuckets} from 'src/shared/utils/prettyBucket'
import {extractBucketLimits} from 'src/cloud/utils/limits'

// Types
import {AppState, Bucket} from 'src/types'
import {SortTypes} from 'src/shared/utils/sort'

interface StateProps {
  buckets: Bucket[]
  limitStatus: LimitStatus
}

interface DispatchProps {
  updateBucket: typeof updateBucket
  deleteBucket: typeof deleteBucket
  checkBucketLimits: typeof checkBucketLimitsAction
}

interface State {
  searchTerm: string
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
      sortKey: 'name',
      sortDirection: Sort.Ascending,
      sortType: SortTypes.String,
    }
  }

  public componentDidMount() {
    this.props.checkBucketLimits()
  }

  public render() {
    const {buckets, limitStatus} = this.props
    const {
      searchTerm,
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
          <CreateBucketButton />
        </SettingsTabbedPageHeader>
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
      </>
    )
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    const sortType = SortTypes.String
    this.setState({sortKey, sortDirection: nextSort, sortType})
  }

  private handleUpdateBucket = (updatedBucket: PrettyBucket) => {
    this.props.updateBucket(updatedBucket as Bucket)
  }

  private handleDeleteBucket = ({id, name}: PrettyBucket) => {
    this.props.deleteBucket(id, name)
  }

  private handleFilterChange = (searchTerm: string): void => {
    this.handleFilterUpdate(searchTerm)
  }

  private handleFilterUpdate = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private get emptyState(): JSX.Element {
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text
            text={`Looks like there aren't any Buckets, why not create one?`}
            highlightWords={['Buckets']}
          />
          <CreateBucketButton />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text text="No Buckets match your query" />
      </EmptyState>
    )
  }
}

const mstp = ({buckets, cloud: {limits}}: AppState): StateProps => ({
  buckets: buckets.list,
  limitStatus: extractBucketLimits(limits),
})

const mdtp = {
  updateBucket,
  deleteBucket,
  checkBucketLimits: checkBucketLimitsAction,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(BucketsTab)
