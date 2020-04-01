// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
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
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

// Constants
import {isSystemBucket} from 'src/buckets/constants/index'

// Types
import {Bucket, Label} from 'src/types'
import {DataLoaderType} from 'src/types/dataLoaders'

// Actions
import {addBucketLabel, deleteBucketLabel} from 'src/buckets/actions/thunks'

interface DispatchProps {
  onAddBucketLabel: typeof addBucketLabel
  onDeleteBucketLabel: typeof deleteBucketLabel
}

interface Props {
  bucket: Bucket
  onEditBucket: (b: Bucket) => void
  onDeleteData: (b: Bucket) => void
  onDeleteBucket: (b: Bucket) => void
  onAddData: (b: Bucket, d: DataLoaderType, l: string) => void
  onUpdateBucket: (b: Bucket) => void
  onFilterChange: (searchTerm: string) => void
}

class BucketCard extends PureComponent<
  Props & WithRouterProps & DispatchProps
> {
  public render() {
    const {bucket, onDeleteBucket} = this.props
    return (
      <ResourceCard
        testID={`bucket-card ${bucket.name}`}
        contextMenu={
          !isSystemBucket(bucket.name) && (
            <BucketContextMenu
              bucket={bucket}
              onDeleteBucket={onDeleteBucket}
            />
          )
        }
        name={this.cardName}
        metaData={this.cardMetaItems}
      >
        {this.actionButtons}
      </ResourceCard>
    )
  }

  private get cardName(): JSX.Element {
    const {bucket} = this.props
    return (
      <ResourceCard.Name
        testID={`bucket--card--name ${bucket.name}`}
        onClick={this.handleNameClick}
        name={bucket.name}
      />
    )
  }

  private get cardMetaItems(): JSX.Element[] {
    const {bucket} = this.props

    const retention = <>Retention: {bucket.readableRetention}</>
    if (bucket.type === 'system') {
      return [
        <span
          className="system-bucket"
          key={`system-bucket-indicator-${bucket.id}`}
        >
          System Bucket
        </span>,
        retention,
      ]
    }

    return [retention]
  }

  private get actionButtons(): JSX.Element {
    const {bucket, onFilterChange} = this.props
    if (bucket.type === 'user') {
      return (
        <FlexBox
          direction={FlexDirection.Row}
          margin={ComponentSize.Small}
          style={{marginTop: '4px'}}
        >
          <InlineLabels
            selectedLabelIDs={bucket.labels}
            onFilterChange={onFilterChange}
            onAddLabel={this.handleAddLabel}
            onRemoveLabel={this.handleRemoveLabel}
          />
          <BucketAddDataButton
            onAddCollector={this.handleAddCollector}
            onAddLineProtocol={this.handleAddLineProtocol}
            onAddScraper={this.handleAddScraper}
          />
          <Button
            text="Settings"
            testID="bucket-settings"
            size={ComponentSize.ExtraSmall}
            onClick={this.handleClickSettings}
          />
          <FeatureFlag name="deleteWithPredicate">
            <Button
              text="Delete Data By Filter"
              testID="bucket-delete-bucket"
              size={ComponentSize.ExtraSmall}
              onClick={this.handleDeleteData}
            />
          </FeatureFlag>
        </FlexBox>
      )
    }
  }

  private handleAddLabel = (label: Label) => {
    const {bucket, onAddBucketLabel} = this.props

    onAddBucketLabel(bucket.id, label)
  }

  private handleRemoveLabel = (label: Label) => {
    const {bucket, onDeleteBucketLabel} = this.props

    onDeleteBucketLabel(bucket.id, label)
  }

  private handleDeleteData = () => {
    const {onDeleteData, bucket} = this.props

    onDeleteData(bucket)
  }

  private handleClickSettings = () => {
    const {
      params: {orgID},
      bucket: {id},
      router,
    } = this.props

    router.push(`/orgs/${orgID}/load-data/buckets/${id}/edit`)
  }

  private handleNameClick = (): void => {
    const {
      params: {orgID},
      bucket: {name},
      router,
    } = this.props

    router.push(`/orgs/${orgID}/data-explorer?bucket=${name}`)
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

const mdtp: DispatchProps = {
  onAddBucketLabel: addBucketLabel,
  onDeleteBucketLabel: deleteBucketLabel,
}

export default connect<{}, DispatchProps>(
  null,
  mdtp
)(withRouter<Props>(BucketCard))
