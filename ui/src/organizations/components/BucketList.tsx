// Libraries
import React, {PureComponent} from 'react'

// Components
import UpdateBucketOverlay from 'src/organizations/components/UpdateBucketOverlay'
import BucketRow, {PrettyBucket} from 'src/organizations/components/BucketRow'
import {OverlayTechnology, IndexList} from 'src/clockface'

// Types
import {OverlayState} from 'src/types/v2'
import DataLoadersWizard from 'src/dataLoaders/components/DataLoadersWizard'
import {DataLoaderStep, DataLoaderType} from 'src/types/v2/dataLoaders'

interface Props {
  buckets: PrettyBucket[]
  emptyState: JSX.Element
  onUpdateBucket: (b: PrettyBucket) => Promise<void>
  onDeleteBucket: (b: PrettyBucket) => Promise<void>
}

interface State {
  bucketID: string
  bucketOverlayState: OverlayState
  dataLoadersOverlayState: OverlayState
}

export default class BucketList extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      bucketID: null,
      bucketOverlayState: OverlayState.Closed,
      dataLoadersOverlayState: OverlayState.Closed,
    }
  }

  public render() {
    const {buckets, emptyState, onDeleteBucket} = this.props

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
              />
            ))}
          </IndexList.Body>
        </IndexList>
        <OverlayTechnology visible={this.isBucketOverlayVisible}>
          <UpdateBucketOverlay
            bucket={this.bucket}
            onCloseModal={this.handleCloseModal}
            onUpdateBucket={this.handleUpdateBucket}
          />
        </OverlayTechnology>
        <DataLoadersWizard
          visible={this.isDataLoadersWizardVisible}
          onCompleteSetup={this.handleDismissDataLoaders}
          bucket={this.bucket}
          buckets={buckets}
          startingStep={DataLoaderStep.Select}
          startingSubstep={0}
          startingType={DataLoaderType.Empty}
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

  private handleStartAddData = (bucket: PrettyBucket) => {
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
    await this.props.onUpdateBucket(updatedBucket)
    this.setState({bucketOverlayState: OverlayState.Closed})
  }
}
