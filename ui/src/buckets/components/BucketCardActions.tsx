// Libraries
import React, {FC} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {
  Button,
  FlexBox,
  FlexDirection,
  ComponentSize,
} from '@influxdata/clockface'
import BucketAddDataButton from 'src/buckets/components/BucketAddDataButton'
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

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
  bucketType: 'user' | 'system'
  orgID: string
  onDeleteData: (b: OwnBucket) => void
  onFilterChange: (searchTerm: string) => void
}

const BucketCardActions: FC<Props &
  RouteComponentProps<{orgID: string}> &
  DispatchProps> = ({
  bucket,
  bucketType,
  orgID,
  onFilterChange,
  onAddBucketLabel,
  onDeleteBucketLabel,
  onDeleteData,
  history,
  onSetDataLoadersBucket,
  onSetDataLoadersType,
}) => {
  if (bucketType === 'system') {
    return null
  }

  const handleAddLabel = (label: Label) => {
    onAddBucketLabel(bucket.id, label)
  }

  const handleRemoveLabel = (label: Label) => {
    onDeleteBucketLabel(bucket.id, label)
  }

  const handleClickSettings = () => {
    history.push(`/orgs/${orgID}/load-data/buckets/${bucket.id}/edit`)
  }

  const handleAddCollector = () => {
    onSetDataLoadersBucket(orgID, bucket.name, bucket.id)

    onSetDataLoadersType(DataLoaderType.Streaming)
    history.push(`/orgs/${orgID}/load-data/buckets/${bucket.id}/telegrafs/new`)
  }

  const handleAddLineProtocol = () => {
    onSetDataLoadersBucket(orgID, bucket.name, bucket.id)

    onSetDataLoadersType(DataLoaderType.LineProtocol)
    history.push(
      `/orgs/${orgID}/load-data/buckets/${bucket.id}/line-protocols/new`
    )
  }

  const handleAddClientLibrary = (): void => {
    onSetDataLoadersBucket(orgID, bucket.name, bucket.id)
    onSetDataLoadersType(DataLoaderType.ClientLibrary)

    history.push(`/orgs/${orgID}/load-data/client-libraries`)
  }

  const handleAddScraper = () => {
    onSetDataLoadersBucket(orgID, bucket.name, bucket.id)

    onSetDataLoadersType(DataLoaderType.Scraping)
    history.push(`/orgs/${orgID}/load-data/buckets/${bucket.id}/scrapers/new`)
  }

  return (
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
)(withRouter(BucketCardActions))
