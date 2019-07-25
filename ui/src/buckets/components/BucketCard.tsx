// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Components
import {ResourceList} from 'src/clockface'
import BucketContextMenu from 'src/buckets/components/BucketContextMenu'

// Types
import {Bucket} from 'src/types'
import {DataLoaderType} from 'src/types/dataLoaders'

export interface PrettyBucket extends Bucket {
  ruleString: string
}

interface Props {
  bucket: PrettyBucket
  onEditBucket: (b: PrettyBucket) => void
  onDeleteData: (b: PrettyBucket) => void
  onDeleteBucket: (b: PrettyBucket) => void
  onAddData: (b: PrettyBucket, d: DataLoaderType, l: string) => void
  onUpdateBucket: (b: PrettyBucket) => void
  onFilterChange: (searchTerm: string) => void
}

class BucketRow extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {bucket, onDeleteBucket, onDeleteData} = this.props
    return (
      <>
        <ResourceList.Card
          testID="resource-card"
          contextMenu={() => (
            <BucketContextMenu
              bucket={bucket}
              onDeleteBucket={onDeleteBucket}
              onDeleteData={onDeleteData}
              onRename={this.handleRenameBucket}
              onAddCollector={this.handleAddCollector}
              onAddLineProtocol={this.handleAddLineProtocol}
              onAddScraper={this.handleAddScraper}
            />
          )}
          name={() => (
            <ResourceList.Name
              hrefValue={this.editBucketLink}
              name={bucket.name}
            />
          )}
          metaData={() => [<>Retention: {bucket.ruleString}</>]}
        />
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
