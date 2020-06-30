// Libraries
import {PureComponent} from 'react'
import {connect} from 'react-redux'
import {WithRouterProps} from 'react-router-dom'

// Types
import {AppState, Organization, ResourceType} from 'src/types'

// Selectors
import {getAll} from 'src/resources/selectors'
import {getOrg} from 'src/organizations/selectors'

interface StateProps {
  orgs: Organization[]
  org: {id?: string}
}

type Props = StateProps & WithRouterProps

class RouteToOrg extends PureComponent<Props> {
  public componentDidMount() {
    const {orgs, router, org} = this.props

    if (!orgs || !orgs.length) {
      router.push(`/no-orgs`)
      return
    }

    // org from local storage
    if (org && org.id) {
      router.push(`/orgs/${org.id}`)
      return
    }

    // else default to first org
    router.push(`/orgs/${orgs[0].id}`)
  }

  render() {
    return false
  }
}

const mstp = (state: AppState): StateProps => {
  const org = getOrg(state)
  const orgs = getAll<Organization>(state, ResourceType.Orgs)

  return {orgs, org}
}

export default connect<StateProps, {}, {}>(mstp)(RouteToOrg)
