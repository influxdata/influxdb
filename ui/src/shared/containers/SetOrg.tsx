// Libraries
import React, {useEffect, useState, FC} from 'react'
import {connect} from 'react-redux'
import {Route, Switch} from 'react-router-dom'

// Components
import {MePage} from 'src/me'
import TasksPage from 'src/tasks/containers/TasksPage'
import TaskPage from 'src/tasks/containers/TaskPage'
import TaskRunsPage from 'src/tasks/components/TaskRunsPage'
import TaskEditPage from 'src/tasks/containers/TaskEditPage'
import DataExplorerPage from 'src/dataExplorer/components/DataExplorerPage'
import DashboardsIndex from 'src/dashboards/components/dashboard_index/DashboardsIndex'
import DashboardContainer from 'src/dashboards/components/DashboardContainer'
import NotebookPage from 'src/notebooks/components/Notebook'
import BucketsIndex from 'src/buckets/containers/BucketsIndex'
import TokensIndex from 'src/authorizations/containers/TokensIndex'

// Types
import {AppState, Organization, ResourceType} from 'src/types'

// Actions
import {setOrg as setOrgAction} from 'src/organizations/actions/creators'

// Utils
import {updateReportingContext} from 'src/cloud/utils/reporting'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

// Decorators
import {RouteComponentProps} from 'react-router-dom'
import {
  RemoteDataState,
  SpinnerContainer,
  TechnoSpinner,
} from '@influxdata/clockface'

// Selectors
import {getAll} from 'src/resources/selectors'

interface PassedInProps {
  children: React.ReactElement<any>
}

interface DispatchProps {
  setOrg: typeof setOrgAction
}

interface StateProps {
  orgs: Organization[]
}

type Props = StateProps &
  DispatchProps &
  PassedInProps &
  RouteComponentProps<{orgID: string}>

const SetOrg: FC<Props> = ({
  match: {
    params: {orgID},
  },
  orgs,
  history,
  setOrg,
}) => {
  const [loading, setLoading] = useState(RemoteDataState.Loading)

  useEffect(() => {
    // does orgID from url match any orgs that exist
    const foundOrg = orgs.find(o => o.id === orgID)
    if (foundOrg) {
      setOrg(foundOrg)
      updateReportingContext({orgID: orgID})
      setLoading(RemoteDataState.Done)
      return
    }
    updateReportingContext({orgID: null})

    if (!orgs.length) {
      history.push(`/no-orgs`)
      return
    }

    // else default to first org
    history.push(`/orgs/${orgs[0].id}`)
  }, [orgID, orgs.length])

  const orgPath = '/orgs/:orgID'

  return (
    <SpinnerContainer loading={loading} spinnerComponent={<TechnoSpinner />}>
      <Switch>
        {/* Tasks */}
        <Route path={`${orgPath}/tasks/:id/runs`} component={TaskRunsPage} />
        <Route path={`${orgPath}/tasks/:id/edit`} component={TaskEditPage} />
        <Route path={`${orgPath}/tasks/new`} component={TaskPage} />
        <Route path={`${orgPath}/tasks`} component={TasksPage} />

        {/* Data Explorer */}
        <Route path={`${orgPath}/data-explorer`} component={DataExplorerPage} />

        {/* Dashboards */}
        <Route
          path={`${orgPath}/dashboards/:dashboardID`}
          component={DashboardContainer}
        />
        <Route path={`${orgPath}/dashboards`} component={DashboardsIndex} />

        {/* Flows */}
        {isFlagEnabled('notebooks') && (
          <Route path={`${orgPath}/notebooks`} component={NotebookPage} />
        )}

        {/* Settings */}
        <Route path={`${orgPath}/load-data/tokens`} component={TokensIndex} />
        <Route path={`${orgPath}/load-data/buckets`} component={BucketsIndex} />
        <Route exact path="/orgs/:orgID" component={MePage} />
      </Switch>
    </SpinnerContainer>
  )
}

const mdtp = {
  setOrg: setOrgAction,
}

const mstp = (state: AppState): StateProps => {
  const orgs = getAll<Organization>(state, ResourceType.Orgs)

  return {orgs}
}

export default connect<StateProps, DispatchProps, PassedInProps>(
  mstp,
  mdtp
)(SetOrg)
