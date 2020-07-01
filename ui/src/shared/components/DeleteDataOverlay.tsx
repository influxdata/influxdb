// Libraries
import React, {FunctionComponent, useEffect} from 'react'
import {connect} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {Overlay, SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Components
import DeleteDataForm from 'src/shared/components/DeleteDataForm/DeleteDataForm'

// Actions
import {
  resetPredicateState,
  setBucketAndKeys,
} from 'src/shared/actions/predicates'

// Types
import {Bucket, AppState, RemoteDataState, ResourceType} from 'src/types'
import {getAll} from 'src/resources/selectors'

interface StateProps {
  buckets: Bucket[]
}

interface DispatchProps {
  resetPredicateState: typeof resetPredicateState
  setBucketAndKeys: typeof setBucketAndKeys
}

type Props = RouteComponentProps<{orgID: string; bucketID: string}> &
  DispatchProps &
  StateProps

const DeleteDataOverlay: FunctionComponent<Props> = ({
  buckets,
  history,
  match: {
    params: {orgID, bucketID},
  },
  resetPredicateState,
  setBucketAndKeys,
}) => {
  const bucket = buckets.find(bucket => bucket.id === bucketID)

  useEffect(() => {
    if (bucket) {
      setBucketAndKeys(bucket.name)
    }
  }, [])

  const handleDismiss = () => {
    resetPredicateState()
    history.push(`/orgs/${orgID}/load-data/buckets/`)
  }

  return (
    <Overlay visible={true}>
      <Overlay.Container maxWidth={600}>
        <Overlay.Header title="Delete Data" onDismiss={handleDismiss} />
        <Overlay.Body>
          <SpinnerContainer
            spinnerComponent={<TechnoSpinner />}
            loading={bucket ? RemoteDataState.Done : RemoteDataState.Loading}
          >
            <DeleteDataForm handleDismiss={handleDismiss} />
          </SpinnerContainer>
        </Overlay.Body>
      </Overlay.Container>
    </Overlay>
  )
}

const mstp = (state: AppState): StateProps => {
  return {
    buckets: getAll<Bucket>(state, ResourceType.Buckets),
  }
}

const mdtp: DispatchProps = {
  resetPredicateState,
  setBucketAndKeys,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter(DeleteDataOverlay))
