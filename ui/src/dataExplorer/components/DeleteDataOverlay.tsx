// Libraries
import React, {FunctionComponent, useEffect} from 'react'
import {connect} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {get} from 'lodash'

// Components
import {Overlay} from '@influxdata/clockface'
import DeleteDataForm from 'src/shared/components/DeleteDataForm/DeleteDataForm'
import GetResources from 'src/resources/components/GetResources'

// Utils
import {getActiveQuery, getActiveTimeMachine} from 'src/timeMachine/selectors'
import {convertTimeRangeToCustom} from 'src/shared/utils/duration'

// Types
import {AppState, TimeRange, ResourceType} from 'src/types'

// Actions
import {
  resetPredicateState,
  setTimeRange,
  setBucketAndKeys,
} from 'src/shared/actions/predicates'

interface StateProps {
  bucketNameFromDE: string
  timeRangeFromDE: TimeRange
}

interface DispatchProps {
  resetPredicateState: typeof resetPredicateState
  setTimeRange: typeof setTimeRange
  setBucketAndKeys: typeof setBucketAndKeys
}

type Props = StateProps & RouteComponentProps<{orgID: string}> & DispatchProps

const DeleteDataOverlay: FunctionComponent<Props> = ({
  history,
  match: {
    params: {orgID},
  },
  bucketNameFromDE,
  timeRangeFromDE,
  resetPredicateState,
  setTimeRange,
  setBucketAndKeys,
}) => {
  useEffect(() => {
    if (bucketNameFromDE) {
      setBucketAndKeys(bucketNameFromDE)
    }
  }, [bucketNameFromDE])

  useEffect(() => {
    if (timeRangeFromDE) {
      setTimeRange(convertTimeRangeToCustom(timeRangeFromDE))
    }
  }, [timeRangeFromDE])

  const handleDismiss = () => {
    resetPredicateState()
    history.push(`/orgs/${orgID}/data-explorer`)
  }

  return (
    <Overlay visible={true}>
      <Overlay.Container maxWidth={600}>
        <Overlay.Header title="Delete Data" onDismiss={handleDismiss} />
        <Overlay.Body>
          <GetResources resources={[ResourceType.Buckets]}>
            <DeleteDataForm handleDismiss={handleDismiss} />
          </GetResources>
        </Overlay.Body>
      </Overlay.Container>
    </Overlay>
  )
}

const mstp = (state: AppState): StateProps => {
  const activeQuery = getActiveQuery(state)
  const bucketNameFromDE = get(activeQuery, 'builderConfig.buckets.0')

  const {timeRange} = getActiveTimeMachine(state)

  return {
    bucketNameFromDE,
    timeRangeFromDE: timeRange,
  }
}

const mdtp: DispatchProps = {
  resetPredicateState,
  setTimeRange,
  setBucketAndKeys,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter(DeleteDataOverlay))
