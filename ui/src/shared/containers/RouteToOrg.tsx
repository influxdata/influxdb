// Libraries
import {FC, useEffect} from 'react'
import {useSelector} from 'react-redux'
import {useHistory} from 'react-router-dom'

// Types
import {AppState, Organization, ResourceType, RemoteDataState} from 'src/types'

// Selectors
import {getAll} from 'src/resources/selectors'
import {getOrg} from 'src/organizations/selectors'

const RouteToOrg: FC = () => {
  const {org, orgs, status} = useSelector((state: AppState) => {
    const org = getOrg(state)
    const orgs = getAll<Organization>(state, ResourceType.Orgs)
    const status = state.resources.orgs.status

    return {
      org,
      orgs,
      status,
    }
  })
  const history = useHistory()

  useEffect(() => {
    if (status !== RemoteDataState.Done) {
      return
    }

    if (!orgs || !orgs.length) {
      history.push(`/no-orgs`)
      return
    }

    // org from local storage
    if (org && org.id) {
      history.push(`/orgs/${org.id}`)
      return
    }

    // else default to first org
    history.push(`/orgs/${orgs[0].id}`)
  }, [history, org, orgs, status])

  return null
}

export default RouteToOrg
