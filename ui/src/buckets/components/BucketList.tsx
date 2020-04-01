// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import {get} from 'lodash'
import memoizeOne from 'memoize-one'

// Components
import BucketCard from 'src/buckets/components/BucketCard'
import {ResourceList} from '@influxdata/clockface'

// Actions
import {setBucketInfo} from 'src/dataLoaders/actions/steps'

// Selectors
import {getSortedResources} from 'src/shared/utils/sort'

// Types
import {OverlayState, Bucket, AppState, DataLoaderType} from 'src/types'
import {setDataLoadersType} from 'src/dataLoaders/actions/dataLoaders'
import {Sort} from '@influxdata/clockface'

// Utils
import {SortTypes} from 'src/shared/utils/sort'

type SortKey = keyof Bucket | 'retentionRules[0].everySeconds'

interface OwnProps {
  buckets: Bucket[]
  emptyState: JSX.Element
  onUpdateBucket: (b: Bucket) => void
  onDeleteBucket: (b: Bucket) => void
  onFilterChange: (searchTerm: string) => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onClickColumn: (
    sortType: SortTypes
  ) => (nextSort: Sort, sortKey: SortKey) => void
}

interface DispatchProps {
  onSetBucketInfo: typeof setBucketInfo
  onSetDataLoadersType: typeof setDataLoadersType
}

interface StateProps {
  dataLoaderType: DataLoaderType
}

type Props = OwnProps & StateProps & DispatchProps

interface State {
  bucketID: string
  bucketOverlayState: OverlayState
}

class BucketList extends PureComponent<Props & WithRouterProps, State> {
  private memGetSortedResources = memoizeOne<typeof getSortedResources>(
    getSortedResources
  )

  constructor(props) {
    super(props)
    const bucketID = get(this, 'props.buckets.0.id', null)

    this.state = {
      bucketID,
      bucketOverlayState: OverlayState.Closed,
    }
  }

  public render() {
    const {emptyState, sortKey, sortDirection, onClickColumn} = this.props
    return (
      <>
        <ResourceList>
          <ResourceList.Header>
            <ResourceList.Sorter
              name="Name"
              sortKey={this.headerKeys[0]}
              sort={sortKey === this.headerKeys[0] ? sortDirection : Sort.None}
              onClick={onClickColumn(SortTypes.String)}
              testID="name-sorter"
            />
            <ResourceList.Sorter
              name="Retention"
              sortKey={this.headerKeys[1]}
              sort={sortKey === this.headerKeys[1] ? sortDirection : Sort.None}
              onClick={onClickColumn(SortTypes.Float)}
              testID="retention-sorter"
            />
          </ResourceList.Header>
          <ResourceList.Body emptyState={emptyState}>
            {this.listBuckets}
          </ResourceList.Body>
        </ResourceList>
      </>
    )
  }

  private get headerKeys(): SortKey[] {
    return ['name', 'retentionRules[0].everySeconds']
  }

  private get listBuckets(): JSX.Element[] {
    const {
      buckets,
      sortKey,
      sortDirection,
      sortType,
      onDeleteBucket,
      onFilterChange,
    } = this.props
    const sortedBuckets = this.memGetSortedResources(
      buckets,
      sortKey,
      sortDirection,
      sortType
    )

    return sortedBuckets.map(bucket => {
      return (
        <BucketCard
          key={bucket.id}
          bucket={bucket}
          onEditBucket={this.handleStartEdit}
          onDeleteBucket={onDeleteBucket}
          onDeleteData={this.handleStartDeleteData}
          onAddData={this.handleStartAddData}
          onUpdateBucket={this.handleUpdateBucket}
          onFilterChange={onFilterChange}
        />
      )
    })
  }

  private handleStartEdit = (bucket: Bucket) => {
    const {orgID} = this.props.params

    this.props.router.push(`/orgs/${orgID}/load-data/buckets/${bucket.id}/edit`)
  }

  private handleStartDeleteData = (bucket: Bucket) => {
    const {orgID} = this.props.params

    this.props.router.push(
      `/orgs/${orgID}/load-data/buckets/${bucket.id}/delete-data`
    )
  }

  private handleStartAddData = (
    bucket: Bucket,
    dataLoaderType: DataLoaderType,
    link: string
  ) => {
    const {onSetBucketInfo, onSetDataLoadersType, router} = this.props
    onSetBucketInfo(bucket.orgID, bucket.name, bucket.id)

    this.setState({
      bucketID: bucket.id,
    })

    onSetDataLoadersType(dataLoaderType)
    router.push(link)
  }

  private handleUpdateBucket = (updatedBucket: Bucket) => {
    this.props.onUpdateBucket(updatedBucket)
    this.setState({bucketOverlayState: OverlayState.Closed})
  }
}

const mstp = (state: AppState): StateProps => {
  return {
    dataLoaderType: state.dataLoading.dataLoaders.type,
  }
}

const mdtp: DispatchProps = {
  onSetBucketInfo: setBucketInfo,
  onSetDataLoadersType: setDataLoadersType,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<Props>(BucketList))
