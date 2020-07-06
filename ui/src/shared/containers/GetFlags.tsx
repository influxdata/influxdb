// Libraries
import React, {useEffect, FC} from 'react'
import {connect} from 'react-redux'
import {Switch, Route} from 'react-router-dom'
import GetOrganizations from 'src/shared/containers/GetOrganizations'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Types
import {RemoteDataState, AppState} from 'src/types'
import {FlagMap} from 'src/shared/reducers/flags'

// Actions
import {getFlags as getFlagsAction} from 'src/shared/actions/flags'

// Utils
import {activeFlags} from 'src/shared/selectors/flags'
import {updateReportingContext} from 'src/cloud/utils/reporting'

interface DispatchProps {
  getFlags: typeof getFlagsAction
}

interface StateProps {
  status: RemoteDataState
  flags: FlagMap
}

type Props = StateProps & DispatchProps

const GetFlags: FC<Props> = ({status, getFlags, flags}) => {
  useEffect(() => {
    if (status === RemoteDataState.NotStarted) {
      getFlags()
    }
  }, [])

  useEffect(() => {
    updateReportingContext(
      Object.entries(flags).reduce((prev, [key, val]) => {
        prev[`flag (${key})`] = val

        return prev
      }, {})
    )
  }, [flags])

  return (
    <SpinnerContainer loading={status} spinnerComponent={<TechnoSpinner />}>
      <Switch>
        <Route component={GetOrganizations} />
      </Switch>
    </SpinnerContainer>
  )
}

const mdtp = {
  getFlags: getFlagsAction,
}

const mstp = (state: AppState): StateProps => ({
  flags: activeFlags(state),
  status: state.flags.status || RemoteDataState.NotStarted,
})

export default connect<StateProps, DispatchProps>(mstp, mdtp)(GetFlags)
