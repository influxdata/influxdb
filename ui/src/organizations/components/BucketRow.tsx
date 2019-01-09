// Libraries
import React, {PureComponent} from 'react'

// Components
import {ComponentSize, IndexList, ConfirmationButton} from 'src/clockface'

// Types
import {OverlayState} from 'src/types/v2'
import {Bucket} from 'src/api'

export interface PrettyBucket extends Bucket {
  ruleString: string
}

interface Props {
  bucket: PrettyBucket
  onEditBucket: (b: PrettyBucket) => void
  onDeleteBucket: (b: PrettyBucket) => void
}

interface State {
  overlayState: OverlayState
}

export default class BucketRow extends PureComponent<Props, State> {
  public render() {
    const {bucket, onDeleteBucket} = this.props
    return (
      <>
        <IndexList.Row>
          <IndexList.Cell>
            <a href="#" onClick={this.handleEditBucket}>
              <span>{bucket.name}</span>
            </a>
          </IndexList.Cell>
          <IndexList.Cell>{bucket.ruleString}</IndexList.Cell>
          <IndexList.Cell>
            <ConfirmationButton
              size={ComponentSize.ExtraSmall}
              text="Delete"
              confirmText="Confirm"
              onConfirm={onDeleteBucket}
              returnValue={bucket}
            />
          </IndexList.Cell>
        </IndexList.Row>
      </>
    )
  }

  private handleEditBucket = (): void => {
    this.props.onEditBucket(this.props.bucket)
  }
}
