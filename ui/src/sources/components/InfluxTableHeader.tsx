import React, {PureComponent} from 'react'
import {Link} from 'react-router'

import {Source} from 'src/types/v2'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  source: Source
}

@ErrorHandling
class InfluxTableHeader extends PureComponent<Props> {
  public render() {
    const {source} = this.props

    return (
      <div className="panel-heading">
        <h2 className="panel-title">
          <span>Connections</span>
        </h2>
        <Link
          to={`/sources/${source.id}/manage-sources/new`}
          className="btn btn-sm btn-primary"
        >
          <span className="icon plus" /> Add Connection
        </Link>
      </div>
    )
  }
}

export default InfluxTableHeader
