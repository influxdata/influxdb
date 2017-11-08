import React, {PropTypes} from 'react'
import _ from 'lodash'
import classnames from 'classnames'
import uuid from 'node-uuid'
import {RULE_ALERT_OPTIONS} from 'src/kapacitor/constants'

const EndpointTabs = ({
  endpointsOnThisAlert,
  selectedEndpoint,
  handleChooseAlert,
  handleRemoveEndpoint,
}) => {
  return endpointsOnThisAlert.length
    ? <ul className="nav nav-tablist nav-tablist-sm nav-tablist-malachite">
        {endpointsOnThisAlert
          .filter(alert => _.get(RULE_ALERT_OPTIONS, alert.type, false))
          .map(alert =>
            <li
              key={uuid.v4()}
              className={classnames({
                active:
                  alert.alias === (selectedEndpoint && selectedEndpoint.alias),
              })}
              onClick={handleChooseAlert(alert)}
            >
              {alert.alias}
              <div
                className="nav-tab--delete"
                onClick={handleRemoveEndpoint(alert)}
              />
            </li>
          )}
      </ul>
    : null
}

const {shape, func, array} = PropTypes

EndpointTabs.propTypes = {
  endpointsOnThisAlert: array,
  selectedEndpoint: shape({}),
  handleChooseAlert: func.isRequired,
  handleRemoveEndpoint: func.isRequired,
}

export default EndpointTabs
