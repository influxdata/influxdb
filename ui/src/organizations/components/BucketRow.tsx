// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  ComponentSize,
  IndexList,
  ConfirmationButton,
  Alignment,
  Button,
  ComponentColor,
} from 'src/clockface'

// Types
import {Bucket} from 'src/api'

export interface PrettyBucket extends Bucket {
  ruleString: string
}

interface Props {
  bucket: PrettyBucket
  onEditBucket: (b: PrettyBucket) => void
  onDeleteBucket: (b: PrettyBucket) => void
  onAddData: (b: PrettyBucket) => void
}

export default class BucketRow extends PureComponent<Props> {
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
          <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
            <ConfirmationButton
              size={ComponentSize.ExtraSmall}
              text="Delete"
              confirmText="Confirm"
              onConfirm={onDeleteBucket}
              returnValue={bucket}
            />
          </IndexList.Cell>
          <IndexList.Cell alignment={Alignment.Right}>
            <Button
              text={'Add Data'}
              onClick={this.handleClickAddData}
              color={ComponentColor.Secondary}
              size={ComponentSize.ExtraSmall}
            />
          </IndexList.Cell>
        </IndexList.Row>
      </>
    )
  }

  private handleEditBucket = (): void => {
    this.props.onEditBucket(this.props.bucket)
  }

  private handleClickAddData = (): void => {
    this.props.onAddData(this.props.bucket)
  }
}
