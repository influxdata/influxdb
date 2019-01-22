// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  ComponentSize,
  ButtonShape,
  IndexList,
  ConfirmationButton,
  Alignment,
  Context,
  ComponentColor,
} from 'src/clockface'

// Types
import {Bucket} from 'src/api'
import {DataLoaderType} from 'src/types/v2/dataLoaders'

export interface PrettyBucket extends Bucket {
  ruleString: string
}

interface Props {
  bucket: PrettyBucket
  onEditBucket: (b: PrettyBucket) => void
  onDeleteBucket: (b: PrettyBucket) => void
  onAddData: (b: PrettyBucket, d: DataLoaderType) => void
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
                text="Add Data"
                shape={ButtonShape.Default}
                color={ComponentColor.Primary}
              >
                <Context.Item
                  label="Configure Telegraf Agent"
                  description="Configure a Telegraf agent to push data into your bucket."
                  action={this.handleAddCollector}
                />
                <Context.Item
                  label="Line Protocol"
                  description="Quickly load an existing line protocol file."
                  action={this.handleAddLineProtocol}
                />
                <Context.Item
                  label="Scrape Metrics"
                  description="Add a scrape target to pull data into your bucket."
                  action={this.handleAddScraper}
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

  private handleAddCollector = (): void => {
    this.props.onAddData(this.props.bucket, DataLoaderType.Streaming)
  }

  private handleAddLineProtocol = (): void => {
    this.props.onAddData(this.props.bucket, DataLoaderType.LineProtocol)
  }

  private handleAddScraper = (): void => {
    this.props.onAddData(this.props.bucket, DataLoaderType.Scraping)
  }
}
