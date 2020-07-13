// Libraries
import React, {FunctionComponent, useEffect} from 'react'
import {connect, ConnectedProps, useDispatch} from 'react-redux'
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
import {AppState, ResourceType} from 'src/types'

// Actions
import {
  resetPredicateState,
  setTimeRange,
  setBucketAndKeys,
} from 'src/shared/actions/predicates'

type ReduxProps = ConnectedProps<typeof connector>
type Props = RouteComponentProps<{orgID: string}> & ReduxProps

const DeleteDataOverlay: FunctionComponent<Props> = ({
  history,
  match: {
    params: {orgID},
  },
  bucketNameFromDE,
  timeRangeFromDE,
}) => {
  const dispatch = useDispatch()

  useEffect(() => {
    if (bucketNameFromDE) {
      dispatch(setBucketAndKeys(bucketNameFromDE))
    }
  }, [dispatch, bucketNameFromDE])

  useEffect(() => {
    if (timeRangeFromDE) {
      dispatch(setTimeRange(convertTimeRangeToCustom(timeRangeFromDE)))
    }
  }, [dispatch, timeRangeFromDE])

  const handleDismiss = () => {
    dispatch(resetPredicateState())
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

const mstp = (state: AppState) => {
  const activeQuery = getActiveQuery(state)
  const bucketNameFromDE = get(activeQuery, 'builderConfig.buckets.0')

  const {timeRange} = getActiveTimeMachine(state)

  return {
    bucketNameFromDE,
    timeRangeFromDE: timeRange,
  }
}

const connector = connect(mstp)

export default connector(withRouter(DeleteDataOverlay))
