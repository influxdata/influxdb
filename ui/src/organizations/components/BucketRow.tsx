// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList, ConfirmationButton, Context} from 'src/clockface'

// Constants
import {DEFAULT_BUCKET_NAME} from 'src/dashboards/constants'

// Types
import {
  Alignment,
  ButtonShape,
  ComponentSize,
  ComponentColor,
  IconFont,
} from '@influxdata/clockface'
import {Bucket} from '@influxdata/influx'
import {DataLoaderType} from 'src/types/v2/dataLoaders'
import EditableName from 'src/shared/components/EditableName'

export interface PrettyBucket extends Bucket {
  ruleString: string
}

interface Props {
  bucket: PrettyBucket
  onEditBucket: (b: PrettyBucket) => void
  onDeleteBucket: (b: PrettyBucket) => void
  onAddData: (b: PrettyBucket, d: DataLoaderType) => void
  onUpdateBucket: (b: PrettyBucket) => void
  onFilterChange: (searchTerm: string) => void
}

export default class BucketRow extends PureComponent<Props> {
  public render() {
    const {bucket, onDeleteBucket} = this.props
    return (
      <>
        <IndexList.Row>
          <IndexList.Cell>
            <EditableName
              onUpdate={this.handleUpdateBucketName}
              name={bucket.name}
              onEditName={this.handleEditBucket}
              noNameString={DEFAULT_BUCKET_NAME}
            />
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
                icon={IconFont.Plus}
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

  private handleUpdateBucketName = async (value: string) => {
    await this.props.onUpdateBucket({...this.props.bucket, name: value})
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
