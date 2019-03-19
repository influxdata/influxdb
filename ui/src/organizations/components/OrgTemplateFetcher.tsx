// Libraries
import React, {useEffect, FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Actions
import {getTemplatesForOrg} from 'src/templates/actions/'

// Types
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types/v2'

interface StateProps {
  templatesStatus: RemoteDataState
}

interface DispatchProps {
  onGetTemplatesForOrg: typeof getTemplatesForOrg
}

interface OwnProps {
  orgName: string
}

type Props = StateProps & DispatchProps & OwnProps

const OrgTemplateFetcher: FunctionComponent<Props> = ({
  orgName,
  templatesStatus,
  onGetTemplatesForOrg,
  children,
}) => {
  useEffect(() => {
    if (templatesStatus === RemoteDataState.NotStarted) {
      onGetTemplatesForOrg(orgName)
    }
  }, [templatesStatus])

  return (
    <SpinnerContainer
      loading={templatesStatus}
      spinnerComponent={<TechnoSpinner />}
    >
      <>{children}</>
    </SpinnerContainer>
  )
}

const mstp = (state: AppState): StateProps => {
  return {templatesStatus: state.templates.status}
}

const mdtp: DispatchProps = {
  onGetTemplatesForOrg: getTemplatesForOrg,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(OrgTemplateFetcher)
