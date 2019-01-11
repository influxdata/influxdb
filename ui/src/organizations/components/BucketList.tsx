// Libraries
import React, {PureComponent} from 'react'

// Components
import UpdateBucketOverlay from 'src/organizations/components/UpdateBucketOverlay'
import BucketRow, {PrettyBucket} from 'src/organizations/components/BucketRow'
import {OverlayTechnology, IndexList} from 'src/clockface'

// Types
import {OverlayState} from 'src/types/v2'

interface Props {
  buckets: PrettyBucket[]
  emptyState: JSX.Element
  onUpdateBucket: (b: PrettyBucket) => Promise<void>
  onDeleteBucket: (b: PrettyBucket) => Promise<void>
}

interface State {
  bucketID: string
  overlayState: OverlayState
}

export default class BucketList extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      bucketID: null,
      overlayState: OverlayState.Closed,
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
              />
            ))}
          </IndexList.Body>
        </IndexList>
        <OverlayTechnology visible={this.isOverlayVisible}>
          <UpdateBucketOverlay
            bucket={this.bucket}
            onCloseModal={this.handleCloseModal}
            onUpdateBucket={this.handleUpdateBucket}
          />
        </OverlayTechnology>
      </>
    )
  }

  private get bucket(): PrettyBucket {
    return this.props.buckets.find(b => b.id === this.state.bucketID)
  }

  private handleCloseModal = () => {
    this.setState({overlayState: OverlayState.Closed})
  }

  private handleStartEdit = (bucket: PrettyBucket) => {
    this.setState({bucketID: bucket.id, overlayState: OverlayState.Open})
  }

  private get isOverlayVisible(): boolean {
    const {bucketID, overlayState} = this.state
    return !!bucketID && overlayState === OverlayState.Open
  }

  private handleUpdateBucket = async (updatedBucket: PrettyBucket) => {
    await this.props.onUpdateBucket(updatedBucket)
    this.setState({overlayState: OverlayState.Closed})
  }
}
