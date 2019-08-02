// Libraries
import React, {FunctionComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import CheckCards from 'src/alerting/components/CheckCards'
import AlertsColumnHeader from 'src/alerting/components/AlertsColumnHeader'

// Types
import {Check, AppState} from 'src/types'

interface StateProps {
  checks: Check[]
}

type Props = StateProps & WithRouterProps

const ChecksColumn: FunctionComponent<Props> = ({
  checks,
  router,
  params: {orgID},
}) => {
  const handleClick = () => {
    router.push(`/orgs/${orgID}/alerting/checks/new`)
  }
  return (
    <>
      <AlertsColumnHeader title="Checks" onCreate={handleClick} />
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
)(withRouter(ChecksColumn))
