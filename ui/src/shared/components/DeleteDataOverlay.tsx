// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'
import {Overlay} from '@influxdata/clockface'

// Components
import DeleteDataForm from 'src/shared/components/DeleteDataForm/DeleteDataForm'

// Types
import {Bucket, AppState} from 'src/types'

interface StateProps {
  buckets: Bucket[]
}

const DeleteDataOverlay: FunctionComponent<StateProps & WithRouterProps> = ({
  router,
  params: {orgID, bucketID},
  buckets,
}) => {
  const handleDismiss = () => router.push(`/orgs/${orgID}/buckets/${bucketID}`)
  const bucketName = buckets.find(bucket => bucket.id === bucketID).name

  return (
    <Overlay visible={true}>
      <Overlay.Container maxWidth={600}>
        <Overlay.Header title="Delete Data" onDismiss={handleDismiss} />
        <Overlay.Body>
          <DeleteDataForm initialBucketName={bucketName} orgID={orgID} />
        </Overlay.Body>
      </Overlay.Container>
    </Overlay>
  )
}

const mstp = (state: AppState): StateProps => {
  return {buckets: state.buckets.list}
}

export default connect<StateProps>(mstp)(
  withRouter<StateProps>(DeleteDataOverlay)
)
