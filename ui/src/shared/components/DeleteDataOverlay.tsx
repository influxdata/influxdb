// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'
import {Overlay, SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Components
import DeleteDataForm from 'src/shared/components/DeleteDataForm/DeleteDataForm'

// Types
import {Bucket, AppState, RemoteDataState} from 'src/types'

// Actions
import {resetPredicateState} from 'src/shared/actions/predicates'

interface StateProps {
  buckets: Bucket[]
}

interface DispatchProps {
  resetPredicateStateAction: () => void
}

type Props = WithRouterProps & DispatchProps & StateProps

const DeleteDataOverlay: FunctionComponent<Props> = ({
  buckets,
  router,
  params: {orgID, bucketID},
  resetPredicateStateAction,
}) => {
  const handleDismiss = () => {
    resetPredicateStateAction()
    router.push(`/orgs/${orgID}/load-data/buckets/${bucketID}`)
  }
  const bucket = buckets.find(bucket => bucket.id === bucketID)
  return (
    <Overlay visible={true}>
      <Overlay.Container maxWidth={600}>
        <Overlay.Header title="Delete Data" onDismiss={handleDismiss} />
        <Overlay.Body>
          <SpinnerContainer
            spinnerComponent={<TechnoSpinner />}
            loading={bucket ? RemoteDataState.Done : RemoteDataState.Loading}
          >
            <DeleteDataForm
              handleDismiss={handleDismiss}
              initialBucketName={bucket && bucket.name}
              orgID={orgID}
            />
          </SpinnerContainer>
        </Overlay.Body>
      </Overlay.Container>
    </Overlay>
  )
}

const mstp = (state: AppState): StateProps => {
  return {
    buckets: state.buckets.list,
  }
}

const mdtp: DispatchProps = {
  resetPredicateStateAction: resetPredicateState,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter<Props>(DeleteDataOverlay))
