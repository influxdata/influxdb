// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import UpdateBucketOverlay from 'src/organizations/components/UpdateBucketOverlay'
import BucketRow, {PrettyBucket} from 'src/organizations/components/BucketRow'
import {Overlay, IndexList} from 'src/clockface'
import DataLoaderSwitcher from 'src/dataLoaders/components/DataLoaderSwitcher'

// Actions
import {setBucketInfo} from 'src/dataLoaders/actions/steps'

// Types
import {OverlayState} from 'src/types'
import {DataLoaderType} from 'src/types/v2/dataLoaders'
import {setDataLoadersType} from 'src/dataLoaders/actions/dataLoaders'
import {AppState} from 'src/types/v2'

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
  dataLoadersOverlayState: OverlayState
}

class BucketList extends PureComponent<Props & WithRouterProps, State> {
  constructor(props) {
    super(props)

    const openDataLoaderOverlay = _.get(
      this,
      'props.location.query.openDataLoaderOverlay',
      false
    )
    const firstBucketID = _.get(this, 'props.buckets.0.id', null)
    const bucketID = openDataLoaderOverlay ? firstBucketID : null

    this.state = {
      bucketID,
      bucketOverlayState: OverlayState.Closed,
      dataLoadersOverlayState: openDataLoaderOverlay
        ? OverlayState.Open
        : OverlayState.Closed,
    }
  }

  public render() {
    const {
      dataLoaderType,
      buckets,
      emptyState,
      onDeleteBucket,
      onFilterChange,
    } = this.props
    const {bucketID} = this.state

    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell columnName="Name" width="30%" />
            <IndexList.HeaderCell columnName="Org" width="30%" />
            <IndexList.HeaderCell columnName="Retention" width="30%" />
            <IndexList.HeaderCell columnName="" width="20%" />
          </IndexList.Header>
          <IndexList.Body columnCount={4} emptyState={emptyState}>
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
        <DataLoaderSwitcher
          type={dataLoaderType}
          visible={this.isDataLoadersWizardVisible}
          onCompleteSetup={this.handleDismissDataLoaders}
          buckets={buckets}
          overrideBucketIDSelection={bucketID}
        />
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
    dataLoaderType: DataLoaderType
  ) => {
    this.props.onSetBucketInfo(
      bucket.organization,
      bucket.organizationID,
      bucket.name,
      bucket.id
    )

    this.props.onSetDataLoadersType(dataLoaderType)

    this.setState({
      bucketID: bucket.id,
      dataLoadersOverlayState: OverlayState.Open,
    })
  }

  private handleDismissDataLoaders = () => {
    this.setState({
      bucketID: '',
      dataLoadersOverlayState: OverlayState.Closed,
    })
  }

  private get isDataLoadersWizardVisible(): boolean {
    const {bucketID, dataLoadersOverlayState} = this.state
    return !!bucketID && dataLoadersOverlayState === OverlayState.Open
  }

  private get isBucketOverlayVisible(): boolean {
    const {bucketID, bucketOverlayState} = this.state
    return !!bucketID && bucketOverlayState === OverlayState.Open
  }

  private handleUpdateBucket = async (updatedBucket: PrettyBucket) => {
    try {
      await this.props.onUpdateBucket(updatedBucket)
      this.setState({bucketOverlayState: OverlayState.Closed})
    } catch (_) {}
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
