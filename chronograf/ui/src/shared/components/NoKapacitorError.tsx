import React, {SFC} from 'react'
import {Link} from 'react-router'

interface Props {
  source: {
    id: string
  }
}

const NoKapacitorError: SFC<Props> = ({source}) => {
  const path = `/sources/${source.id}/kapacitors/new`
  return (
    <div className="graph-empty">
      <p>
        The current source does not have an associated Kapacitor instance
        <br />
        <br />
        <Link to={path} className="btn btn-sm btn-primary">
          Configure Kapacitor
        </Link>
      </p>
    </div>
  )
}

export default NoKapacitorError
