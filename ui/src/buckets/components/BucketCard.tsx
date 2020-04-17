// Libraries
import React, {FC} from 'react'
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

// Actions
import {addBucketLabel, deleteBucketLabel} from 'src/buckets/actions/thunks'
import {setBucketInfo} from 'src/dataLoaders/actions/steps'
import {setDataLoadersType} from 'src/dataLoaders/actions/dataLoaders'

// Types
import {Label, OwnBucket} from 'src/types'
import {DataLoaderType} from 'src/types/dataLoaders'

interface DispatchProps {
  onAddBucketLabel: typeof addBucketLabel
  onDeleteBucketLabel: typeof deleteBucketLabel
  onSetDataLoadersBucket: typeof setBucketInfo
  onSetDataLoadersType: typeof setDataLoadersType
}

interface Props {
  bucket: OwnBucket
  onDeleteData: (b: OwnBucket) => void
  onDeleteBucket: (b: OwnBucket) => void
  onUpdateBucket: (b: OwnBucket) => void
  onFilterChange: (searchTerm: string) => void
}

const BucketCard: FC<Props & WithRouterProps & DispatchProps> = ({
  bucket,
  onDeleteBucket,
  onFilterChange,
  onAddBucketLabel,
  onDeleteBucketLabel,
  onDeleteData,
  router,
  params: {orgID},
  onSetDataLoadersBucket,
  onSetDataLoadersType,
}) => {
  const handleAddLabel = (label: Label) => {
    onAddBucketLabel(bucket.id, label)
  }

  const handleRemoveLabel = (label: Label) => {
    onDeleteBucketLabel(bucket.id, label)
  }

  const handleClickSettings = () => {
    router.push(`/orgs/${orgID}/load-data/buckets/${bucket.id}/edit`)
  }

  const handleNameClick = () => {
    router.push(`/orgs/${orgID}/data-explorer?bucket=${bucket.name}`)
  }

  const handleAddCollector = () => {
    onSetDataLoadersBucket(orgID, bucket.name, bucket.id)

    onSetDataLoadersType(DataLoaderType.Streaming)
    router.push(`/orgs/${orgID}/load-data/buckets/${bucket.id}/telegrafs/new`)
  }

  const handleAddLineProtocol = () => {
    onSetDataLoadersBucket(orgID, bucket.name, bucket.id)

    onSetDataLoadersType(DataLoaderType.LineProtocol)
    router.push(
      `/orgs/${orgID}/load-data/buckets/${bucket.id}/line-protocols/new`
    )
  }

  const handleAddClientLibrary = (): void => {
    onSetDataLoadersBucket(orgID, bucket.name, bucket.id)
    onSetDataLoadersType(DataLoaderType.ClientLibrary)

    router.push(`/orgs/${orgID}/load-data/client-libraries`)
  }

  const handleAddScraper = () => {
    onSetDataLoadersBucket(orgID, bucket.name, bucket.id)

    onSetDataLoadersType(DataLoaderType.Scraping)
    router.push(`/orgs/${orgID}/load-data/buckets/${bucket.id}/scrapers/new`)
  }

  const actionButtons = (
    <FlexBox
      direction={FlexDirection.Row}
      margin={ComponentSize.Small}
      style={{marginTop: '4px'}}
    >
      <InlineLabels
        selectedLabelIDs={bucket.labels}
        onFilterChange={onFilterChange}
        onAddLabel={handleAddLabel}
        onRemoveLabel={handleRemoveLabel}
      />
      <BucketAddDataButton
        onAddCollector={handleAddCollector}
        onAddLineProtocol={handleAddLineProtocol}
        onAddClientLibrary={handleAddClientLibrary}
        onAddScraper={handleAddScraper}
      />
      <Button
        text="Settings"
        testID="bucket-settings"
        size={ComponentSize.ExtraSmall}
        onClick={handleClickSettings}
      />
      <FeatureFlag name="deleteWithPredicate">
        <Button
          text="Delete Data By Filter"
          testID="bucket-delete-bucket"
          size={ComponentSize.ExtraSmall}
          onClick={() => onDeleteData(bucket)}
        />
      </FeatureFlag>
    </FlexBox>
  )

  return (
    <ResourceCard
      testID={`bucket-card ${bucket.name}`}
      contextMenu={
        !isSystemBucket(bucket.name) && (
          <BucketContextMenu bucket={bucket} onDeleteBucket={onDeleteBucket} />
        )
      }
    >
      <ResourceCard.Name
        testID={`bucket--card--name ${bucket.name}`}
        onClick={handleNameClick}
        name={bucket.name}
      />
      <ResourceCard.Meta>
        {bucket.type !== 'user' && (
          <span
            className="system-bucket"
            key={`system-bucket-indicator-${bucket.id}`}
          >
            System Bucket
          </span>
        )}
        <span data-testid="bucket-retention">
          Retention: {bucket.readableRetention}
        </span>
      </ResourceCard.Meta>
      {bucket.type === 'user' && actionButtons}
    </ResourceCard>
  )
}

const mdtp: DispatchProps = {
  onAddBucketLabel: addBucketLabel,
  onDeleteBucketLabel: deleteBucketLabel,
  onSetDataLoadersBucket: setBucketInfo,
  onSetDataLoadersType: setDataLoadersType,
}

export default connect<{}, DispatchProps>(
  null,
  mdtp
)(withRouter<Props>(BucketCard))
