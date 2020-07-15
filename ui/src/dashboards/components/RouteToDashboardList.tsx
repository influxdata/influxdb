import {FC} from 'react'
import {RouteComponentProps} from 'react-router-dom'

const RouteToDashboardList: FC<RouteComponentProps<{orgID: string}>> = ({
  history,
  match,
}) => {
  history.push(`/orgs/${match.params.orgID}/dashboards-list`)
  return null
}

export default RouteToDashboardList
