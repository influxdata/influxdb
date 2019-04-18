// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps, Link} from 'react-router'
import _ from 'lodash'

// Components
import {IndexList, ConfirmationButton, Context} from 'src/clockface'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'

// Types
import {
  ButtonShape,
  ComponentSize,
  ComponentColor,
  IconFont,
  Button,
  ComponentSpacer,
  AlignItems,
  FlexDirection,
} from '@influxdata/clockface'
import {Alignment} from 'src/clockface'
import {Bucket} from 'src/types'
import {DataLoaderType} from 'src/types/dataLoaders'

export interface PrettyBucket extends Bucket {
  ruleString: string
}

interface Props {
  bucket: PrettyBucket
  onEditBucket: (b: PrettyBucket) => void
  onDeleteBucket: (b: PrettyBucket) => void
  onAddData: (b: PrettyBucket, d: DataLoaderType, l: string) => void
  onUpdateBucket: (b: PrettyBucket) => void
  onFilterChange: (searchTerm: string) => void
}

class BucketRow extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {bucket, onDeleteBucket} = this.props

    return (
      <>
        <IndexList.Row>
          <IndexList.Cell>
            <div className="editable-name">
              <Link to={this.editBucketLink}>
                <span>{bucket.name}</span>
              </Link>
            </div>
          </IndexList.Cell>
          <IndexList.Cell>{bucket.ruleString}</IndexList.Cell>
          <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
            <ComponentSpacer
              alignItems={AlignItems.Center}
              direction={FlexDirection.Row}
              margin={ComponentSize.Small}
            >
              <Button
                text="Rename"
                onClick={this.handleRenameBucket}
                color={ComponentColor.Danger}
                size={ComponentSize.ExtraSmall}
              />
              <ConfirmationButton
                size={ComponentSize.ExtraSmall}
                text="Delete"
                confirmText="Confirm"
                onConfirm={onDeleteBucket}
                returnValue={bucket}
              />
            </ComponentSpacer>
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
                <CloudExclude>
                  <Context.Item
                    label="Scrape Metrics"
                    description="Add a scrape target to pull data into your bucket."
                    action={this.handleAddScraper}
                  />
                </CloudExclude>
              </Context.Menu>
            </Context>
          </IndexList.Cell>
        </IndexList.Row>
      </>
    )
  }

  private handleRenameBucket = () => {
    const {
      params: {orgID},
      bucket: {id},
      router,
    } = this.props

    router.push(`/orgs/${orgID}/buckets/${id}/rename`)
  }

  private get editBucketLink(): string {
    const {
      params: {orgID},
      bucket: {id},
    } = this.props

    return `/orgs/${orgID}/buckets/${id}/edit`
  }

  private handleAddCollector = (): void => {
    const {
      params: {orgID},
      bucket: {id},
    } = this.props

    const link = `/orgs/${orgID}/buckets/${id}/telegrafs/new`
    this.props.onAddData(this.props.bucket, DataLoaderType.Streaming, link)
  }

  private handleAddLineProtocol = (): void => {
    const {
      params: {orgID},
      bucket: {id},
    } = this.props

    const link = `/orgs/${orgID}/buckets/${id}/line-protocols/new`
    this.props.onAddData(this.props.bucket, DataLoaderType.LineProtocol, link)
  }

  private handleAddScraper = (): void => {
    const {
      params: {orgID},
      bucket: {id},
    } = this.props

    const link = `/orgs/${orgID}/buckets/${id}/scrapers/new`
    this.props.onAddData(this.props.bucket, DataLoaderType.Scraping, link)
  }
}

export default withRouter<Props>(BucketRow)
