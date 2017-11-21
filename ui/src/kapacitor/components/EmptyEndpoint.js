import React from 'react'
import {Link} from 'react-router'

const EmptyEndpoint = ({configLink}) => {
  return (
    <div className="endpoint-tab-contents">
      <div className="endpoint-tab--parameters">
        <div className="form-group">
          This endpoint does not seem to be configured.
          <Link to={configLink} title="Configuration Page">
            Configure it here.
          </Link>
        </div>
      </div>
    </div>
  )
}

export default EmptyEndpoint
