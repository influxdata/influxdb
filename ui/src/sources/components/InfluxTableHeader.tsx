import React, {PureComponent, ReactElement} from 'react'
import {Link} from 'react-router'

import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'

import {Me, Source} from 'src/types'

interface Props {
  me: Me
  source: Source
  isUsingAuth: boolean
}

class InfluxTableHeader extends PureComponent<Props> {
  public render() {
    const {source} = this.props

    return (
      <div className="panel-heading">
        <h2 className="panel-title">{this.title}</h2>
        <Authorized requiredRole={EDITOR_ROLE}>
          <Link
            to={`/sources/${source.id}/manage-sources/new`}
            className="btn btn-sm btn-primary"
          >
            <span className="icon plus" /> Add Connection
          </Link>
        </Authorized>
      </div>
    )
  }

  private get title(): ReactElement<HTMLSpanElement> {
    const {isUsingAuth, me} = this.props
    if (isUsingAuth) {
      return (
        <span>
          Connections for <em>{me.currentOrganization.name}</em>
        </span>
      )
    }

    return <span>Connections</span>
  }
}

export default InfluxTableHeader
