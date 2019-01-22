// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  ComponentSize,
  ButtonShape,
  IndexList,
  ConfirmationButton,
  Alignment,
  IconFont,
  Context,
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
            <Context align={Alignment.Center}>
              <Context.Menu
                icon={IconFont.Pencil}
                text="Add Data"
                shape={ButtonShape.Default}
                color={ComponentColor.Secondary}
                buttonColor={ComponentColor.Secondary}
              >
                <Context.Item
                  label="Configure Agent"
                  description="Configure a Telegraf agent to push data into your bucket."
                  action={this.handleClickAddData}
                />
                <Context.Item
                  label="Line Protocol"
                  description="Quickly load an existing line protocol file."
                  action={this.handleClickAddData}
                />
                <Context.Item
                  label="Scrape Metrics"
                  description="Add a scrape target to pull data into your bucket."
                  action={this.handleClickAddData}
                />
              </Context.Menu>
            </Context>
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
