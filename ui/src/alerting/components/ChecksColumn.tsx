// Libraries
import React, {FunctionComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Selectors
import {viewableLabels} from 'src/labels/selectors'

// Components
import CheckCards from 'src/alerting/components/CheckCards'
import AlertsColumnHeader from 'src/alerting/components/AlertsColumn'

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

  const tooltipContents = (
    <>
      A <strong>Check</strong> is a periodic query that the system
      <br />
      performs against your time series data
      <br />
      that will generate a status
    </>
  )

  return (
    <AlertsColumnHeader
      title="Checks"
      onCreate={handleClick}
      questionMarkTooltipContents={tooltipContents}
    >
      <CheckCards checks={checks} />
    </AlertsColumnHeader>
  )
}

const mstp = (state: AppState) => {
  const {
    checks: {list: checks},
    labels: {list: labels},
  } = state

  return {checks, labels: viewableLabels(labels)}
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(withRouter(ChecksColumn))
