// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Types
import {Check, AppState} from 'src/types'
import CheckCards from 'src/alerting/components/CheckCards'

interface StateProps {
  checks: Check[]
}

type Props = StateProps

const ChecksColumn: FunctionComponent<Props> = ({checks}) => {
  return (
    <>
      Checks
      <CheckCards checks={checks} />
    </>
  )
}

const mstp = (state: AppState) => {
  const {
    checks: {list: checks},
  } = state

  return {checks}
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(ChecksColumn)
