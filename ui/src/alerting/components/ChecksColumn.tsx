// Libraries
import React, {FunctionComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Selectors
import {viewableLabels} from 'src/labels/selectors'

// Components
import {Button, IconFont, ComponentColor} from '@influxdata/clockface'
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

  const createButton = (
    <Button
      color={ComponentColor.Primary}
      text="Create"
      onClick={handleClick}
      testID="create-check"
      icon={IconFont.Plus}
    />
  )

  return (
    <AlertsColumnHeader
      title="Checks"
      createButton={createButton}
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
