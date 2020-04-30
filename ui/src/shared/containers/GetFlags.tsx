// Libraries
import React, {useEffect, FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Types
import {RemoteDataState, AppState} from 'src/types'

// Actions
import {getFlags as getFlagsAction} from 'src/shared/actions/flags'

interface PassedInProps {
  children: React.ReactElement<any>
}

interface DispatchProps {
  getFlags: typeof getFlagsAction
}

interface StateProps {
  status: RemoteDataState
}

type Props = StateProps & DispatchProps & PassedInProps

const GetFlags: FunctionComponent<Props> = ({status, getFlags, children}) => {
  useEffect(() => {
    if (status === RemoteDataState.NotStarted) {
      getFlags()
    }
  }, [])

  return (
    <SpinnerContainer loading={status} spinnerComponent={<TechnoSpinner />}>
      {children && React.cloneElement(children)}
    </SpinnerContainer>
  )
}

const mdtp = {
  getFlags: getFlagsAction,
}

const mstp = (state: AppState): StateProps => ({
  status: state.flags.status || RemoteDataState.NotStarted,
})

export default connect<StateProps, DispatchProps, PassedInProps>(
  mstp,
  mdtp
)(GetFlags)
