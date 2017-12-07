import React, {PropTypes} from 'react'
import classnames from 'classnames'
import uuid from 'node-uuid'

const HandlerTabs = ({
  handlersOnThisAlert,
  selectedHandler,
  handleChooseHandler,
  handleRemoveHandler,
}) => {
  return handlersOnThisAlert.length
    ? <ul className="endpoint-tabs">
        {handlersOnThisAlert.map(ep =>
          <li
            key={uuid.v4()}
            className={classnames('endpoint-tab', {
              active: ep.alias === (selectedHandler && selectedHandler.alias),
            })}
            onClick={handleChooseHandler(ep)}
          >
            {ep.type}
            <button
              className="endpoint-tab--delete"
              onClick={handleRemoveHandler(ep)}
            />
          </li>
        )}
      </ul>
    : null
}

const {shape, func, array} = PropTypes

HandlerTabs.propTypes = {
  handlersOnThisAlert: array,
  selectedHandler: shape({}),
  handleChooseHandler: func.isRequired,
  handleRemoveHandler: func.isRequired,
}

export default HandlerTabs
