// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Components
import {ResourceCard} from '@influxdata/clockface'
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
        <ResourceCard
          testID="bucket--card"
          contextMenu={
            <BucketContextMenu
              bucket={bucket}
              onDeleteBucket={onDeleteBucket}
              onDeleteData={onDeleteData}
              onRename={this.handleRenameBucket}
              onAddCollector={this.handleAddCollector}
              onAddLineProtocol={this.handleAddLineProtocol}
              onAddScraper={this.handleAddScraper}
            />
          }
          name={
            <ResourceCard.Name
              testID={`bucket--card ${bucket.name}`}
              onClick={this.handleNameClick}
              name={bucket.name}
            />
          }
          metaData={[<>Retention: {bucket.ruleString}</>]}
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

    router.push(`/orgs/${orgID}/load-data/buckets/${id}/rename`)
  }

  private handleNameClick = (): void => {
    const {
      params: {orgID},
      bucket: {id},
      router,
    } = this.props

    router.push(`/orgs/${orgID}/load-data/buckets/${id}/edit`)
  }

  private handleAddCollector = (): void => {
    const {
      params: {orgID},
      bucket: {id},
    } = this.props

    const link = `/orgs/${orgID}/load-data/buckets/${id}/telegrafs/new`
    this.props.onAddData(this.props.bucket, DataLoaderType.Streaming, link)
  }

  private handleAddLineProtocol = (): void => {
    const {
      params: {orgID},
      bucket: {id},
    } = this.props

    const link = `/orgs/${orgID}/load-data/buckets/${id}/line-protocols/new`
    this.props.onAddData(this.props.bucket, DataLoaderType.LineProtocol, link)
  }

  private handleAddScraper = (): void => {
    const {
      params: {orgID},
      bucket: {id},
    } = this.props

    const link = `/orgs/${orgID}/load-data/buckets/${id}/scrapers/new`
    this.props.onAddData(this.props.bucket, DataLoaderType.Scraping, link)
  }
}

export default withRouter<Props>(BucketRow)
