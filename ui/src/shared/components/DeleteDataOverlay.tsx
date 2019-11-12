// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'
import {Overlay} from '@influxdata/clockface'
import {get} from 'lodash'

// Components
import DeleteDataForm from 'src/shared/components/DeleteDataForm/DeleteDataForm'

// Types
import {Bucket, AppState} from 'src/types'

// Utils
import {
  getActiveQuery,
  getTagKeys,
  getTagValues,
} from 'src/timeMachine/selectors'

interface StateProps {
  buckets: Bucket[]
  selectedBucketName?: string
  selectedKeys: string[]
  selectedValues: (string | number)[]
}

const DeleteDataOverlay: FunctionComponent<StateProps & WithRouterProps> = ({
  buckets,
  router,
  params: {orgID, bucketID},
  selectedBucketName,
  selectedKeys,
  selectedValues,
}) => {
  const handleDismiss = () =>
    router.push(`/orgs/${orgID}/load-data/buckets/${bucketID}`)
  // separated find logic and name logic since directly routing the a delete-data
  // endpoint was crashing the app because the bucket is undefined until the component mounts
  const bucket = buckets.find(bucket => bucket.id === bucketID)
  const bucketName = bucket && bucket.name ? bucket.name : ''
  const initialBucketName = selectedBucketName || bucketName
  return (
    <Overlay visible={true}>
      <Overlay.Container maxWidth={600}>
        <Overlay.Header title="Delete Data" onDismiss={handleDismiss} />
        <Overlay.Body>
          <DeleteDataForm
            handleDismiss={handleDismiss}
            initialBucketName={initialBucketName}
            keys={selectedKeys}
            orgID={orgID}
            values={selectedValues}
          />
        </Overlay.Body>
      </Overlay.Container>
    </Overlay>
  )
}

const mstp = (state: AppState): StateProps => {
  const activeQuery = getActiveQuery(state)
  const selectedBucketName = get(activeQuery, 'builderConfig.buckets.0')
  const selectedBucketTags = get(activeQuery, 'builderConfig.tags')
  const selectedKeys = getTagKeys(selectedBucketTags)
  const selectedValues = getTagValues(selectedBucketTags)
  return {
    buckets: state.buckets.list,
    selectedBucketName,
    selectedKeys,
    selectedValues,
  }
}

export default connect<StateProps>(mstp)(
  withRouter<StateProps>(DeleteDataOverlay)
)
