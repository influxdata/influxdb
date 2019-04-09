// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import UpdateBucketOverlay from 'src/buckets/components/UpdateBucketOverlay'
import BucketRow, {PrettyBucket} from 'src/buckets/components/BucketRow'
import {Overlay, IndexList} from 'src/clockface'

// Actions
import {setBucketInfo} from 'src/dataLoaders/actions/steps'

// Types
import {OverlayState} from 'src/types'
import {DataLoaderType} from 'src/types/dataLoaders'
import {setDataLoadersType} from 'src/dataLoaders/actions/dataLoaders'
import {AppState} from 'src/types'

interface OwnProps {
  buckets: PrettyBucket[]
  emptyState: JSX.Element
  onUpdateBucket: (b: PrettyBucket) => void
  onDeleteBucket: (b: PrettyBucket) => void
  onFilterChange: (searchTerm: string) => void
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
  constructor(props) {
    super(props)
    const bucketID = get(this, 'props.buckets.0.id', null)

    this.state = {
      bucketID,
      bucketOverlayState: OverlayState.Closed,
    }
  }

  public render() {
    const {buckets, emptyState, onDeleteBucket, onFilterChange} = this.props

    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell columnName="Name" width="40%" />
            <IndexList.HeaderCell columnName="Retention" width="40%" />
            <IndexList.HeaderCell columnName="" width="20%" />
          </IndexList.Header>
          <IndexList.Body columnCount={3} emptyState={emptyState}>
            {buckets.map(bucket => (
              <BucketRow
                key={bucket.id}
                bucket={bucket}
                onEditBucket={this.handleStartEdit}
                onDeleteBucket={onDeleteBucket}
                onAddData={this.handleStartAddData}
                onUpdateBucket={this.handleUpdateBucket}
                onFilterChange={onFilterChange}
              />
            ))}
          </IndexList.Body>
        </IndexList>
        <Overlay visible={this.isBucketOverlayVisible}>
          <UpdateBucketOverlay
            bucket={this.bucket}
            onCloseModal={this.handleCloseModal}
            onUpdateBucket={this.handleUpdateBucket}
          />
        </Overlay>
      </>
    )
  }

  private get bucket(): PrettyBucket {
    return this.props.buckets.find(b => b.id === this.state.bucketID)
  }

  private handleCloseModal = () => {
    this.setState({bucketOverlayState: OverlayState.Closed})
  }

  private handleStartEdit = (bucket: PrettyBucket) => {
    this.setState({bucketID: bucket.id, bucketOverlayState: OverlayState.Open})
  }

  private handleStartAddData = (
    bucket: PrettyBucket,
    dataLoaderType: DataLoaderType,
    link: string
  ) => {
    const {onSetBucketInfo, onSetDataLoadersType, router} = this.props
    onSetBucketInfo(
      bucket.organization,
      bucket.organizationID,
      bucket.name,
      bucket.id
    )

    this.setState({
      bucketID: bucket.id,
    })

    onSetDataLoadersType(dataLoaderType)
    router.push(link)
  }

  private get isBucketOverlayVisible(): boolean {
    const {bucketID, bucketOverlayState} = this.state
    return !!bucketID && bucketOverlayState === OverlayState.Open
  }

  private handleUpdateBucket = async (updatedBucket: PrettyBucket) => {
    await this.props.onUpdateBucket(updatedBucket)
    this.setState({bucketOverlayState: OverlayState.Closed})
  }
}

const mstp = ({
  dataLoading: {
    dataLoaders: {type},
  },
}: AppState): StateProps => ({
  dataLoaderType: type,
})

const mdtp: DispatchProps = {
  onSetBucketInfo: setBucketInfo,
  onSetDataLoadersType: setDataLoadersType,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<Props>(BucketList))
