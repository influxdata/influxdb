// Libraries
import {PureComponent} from 'react'
import {connect} from 'react-redux'
import {WithRouterProps} from 'react-router'

// Types
import {AppState} from 'src/types'
import {Organization} from '@influxdata/influx'

// Decorators

interface StateProps {
  orgs: Organization[]
  org: {id?: string}
}

type Props = StateProps & WithRouterProps

class RouteToOrg extends PureComponent<Props> {
  public componentDidMount() {
    const {orgs, router, org} = this.props

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
  const {
    orgs: {items, org},
  } = state

  return {orgs: items, org}
}

export default connect<StateProps, {}, {}>(mstp)(RouteToOrg)
