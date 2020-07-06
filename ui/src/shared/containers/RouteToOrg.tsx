// Libraries
import {PureComponent} from 'react'
import {connect} from 'react-redux'
import {RouteComponentProps} from 'react-router-dom'

// Types
import {AppState, Organization, ResourceType} from 'src/types'

// Selectors
import {getAll} from 'src/resources/selectors'
import {getOrg} from 'src/organizations/selectors'

interface StateProps {
  orgs: Organization[]
  org: {id?: string}
}

type Props = StateProps & RouteComponentProps

class RouteToOrg extends PureComponent<Props> {
  public componentDidMount() {
    const {orgs, history, org} = this.props

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

export default connect<StateProps>(mstp)(RouteToOrg)
