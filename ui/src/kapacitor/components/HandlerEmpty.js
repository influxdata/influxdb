import React, {PropTypes} from 'react'
import {Link} from 'react-router'

const HandlerEmpty = ({configLink}) => {
  return (
    <div className="endpoint-tab-contents">
      <div className="endpoint-tab--parameters">
        <div className="form-group">
          This handler does not seem to be configured.
          <Link to={configLink} title="Configuration Page">
            Configure it here.
          </Link>
        </div>
      </div>
    </div>
  )
}

const {string} = PropTypes

HandlerEmpty.propTypes = {
  configLink: string,
}

export default HandlerEmpty
