// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Components
import {
  Button,
  ResourceCard,
  FlexBox,
  FlexDirection,
  ComponentSize,
} from '@influxdata/clockface'
import BucketContextMenu from 'src/buckets/components/BucketContextMenu'
import BucketAddDataButton from 'src/buckets/components/BucketAddDataButton'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

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
    const {bucket, onDeleteBucket} = this.props
    return (
      <ResourceCard
        testID="bucket--card"
        contextMenu={
          <BucketContextMenu bucket={bucket} onDeleteBucket={onDeleteBucket} />
        }
        name={
          <ResourceCard.Name
            testID={`bucket--card ${bucket.name}`}
            onClick={this.handleNameClick}
            name={bucket.name}
          />
        }
        metaData={[<>Retention: {bucket.ruleString}</>]}
      >
        <FlexBox
          direction={FlexDirection.Row}
          margin={ComponentSize.Small}
          style={{marginTop: '4px'}}
        >
          <BucketAddDataButton
            onAddCollector={this.handleAddCollector}
            onAddLineProtocol={this.handleAddLineProtocol}
            onAddScraper={this.handleAddScraper}
          />
          {this.renameButton}
          <FeatureFlag name="deleteWithPredicate">
            <Button
              text="Delete Data By Filter"
              testID="bucket-delete-task"
              size={ComponentSize.ExtraSmall}
              onClick={this.handleDeleteData}
            />
          </FeatureFlag>
        </FlexBox>
      </ResourceCard>
    )
  }

  private get renameButton() {
    const {bucket} = this.props
    if (bucket.type === 'user') {
      return (
        <Button
          text="Rename"
          testID="bucket-rename"
          size={ComponentSize.ExtraSmall}
          onClick={this.handleRenameBucket}
        />
      )
    } else {
      return null
    }
  }

  private handleDeleteData = () => {
    const {onDeleteData, bucket} = this.props

    onDeleteData(bucket)
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
