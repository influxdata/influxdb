import React, {PropTypes} from 'react'
import {Link} from 'react-router'

const HandlerEmpty = ({configLink}) =>
  <div className="endpoint-tab-contents">
    <div className="endpoint-tab--parameters">
      <div className="endpoint-tab--parameters--empty">
        <p>This handler does not seem to be configured.</p>
        <Link to={configLink} title="Configuration Page">
          <div className="form-group-submit col-xs-12 text-center">
            <button className="btn btn-primary" type="submit">
              Configure Alert Handlers
            </button>
          </div>
        </Link>
      </div>
    </div>
  </div>

const {string} = PropTypes

HandlerEmpty.propTypes = {
  configLink: string,
}

export default HandlerEmpty
