// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'
import {get} from 'lodash'

// Components
import {Overlay} from '@influxdata/clockface'
import DeleteDataForm from 'src/shared/components/DeleteDataForm/DeleteDataForm'
import GetResources, {ResourceType} from 'src/shared/components/GetResources'

// Utils
import {getActiveQuery, getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState, TimeRange} from 'src/types'

// Actions
import {resetPredicateState} from 'src/shared/actions/predicates'

interface StateProps {
  selectedBucketName?: string
  timeRange: TimeRange
}

interface DispatchProps {
  resetPredicateState: () => void
}

type Props = StateProps & WithRouterProps & DispatchProps

const DeleteDataOverlay: FunctionComponent<Props> = ({
  router,
  params: {orgID},
  selectedBucketName,
  timeRange,
  resetPredicateState,
}) => {
  const handleDismiss = () => {
    resetPredicateState()
    router.push(`/orgs/${orgID}/data-explorer`)
  }

  return (
    <Overlay visible={true}>
      <Overlay.Container maxWidth={600}>
        <Overlay.Header title="Delete Data" onDismiss={handleDismiss} />
        <Overlay.Body>
          <GetResources resources={[ResourceType.Buckets]}>
            <DeleteDataForm
              handleDismiss={handleDismiss}
              initialBucketName={selectedBucketName}
              initialTimeRange={timeRange}
              orgID={orgID}
            />
          </GetResources>
        </Overlay.Body>
      </Overlay.Container>
    </Overlay>
  )
}

const mstp = (state: AppState): StateProps => {
  const activeQuery = getActiveQuery(state)
  const selectedBucketName = get(activeQuery, 'builderConfig.buckets.0')

  const {timeRange} = getActiveTimeMachine(state)

  return {
    selectedBucketName,
    timeRange,
  }
}

const mdtp: DispatchProps = {
  resetPredicateState,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter<Props>(DeleteDataOverlay))
