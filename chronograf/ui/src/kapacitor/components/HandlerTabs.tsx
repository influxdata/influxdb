import React, {SFC, MouseEvent} from 'react'
import classnames from 'classnames'
import uuid from 'uuid'

import {Handler} from 'src/types/kapacitor'

interface HandlerWithText extends Handler {
  text: string
}

interface Props {
  handlersOnThisAlert: HandlerWithText[]
  selectedHandler: HandlerWithText
  handleChooseHandler: (
    ep: HandlerWithText
  ) => (event: MouseEvent<HTMLLIElement>) => void
  handleRemoveHandler: (
    ep: HandlerWithText
  ) => (event: MouseEvent<HTMLButtonElement>) => void
}

const HandlerTabs: SFC<Props> = ({
  handlersOnThisAlert,
  selectedHandler,
  handleChooseHandler,
  handleRemoveHandler,
}) =>
  handlersOnThisAlert.length ? (
    <ul className="endpoint-tabs">
      {handlersOnThisAlert.map(endpoint => {
        return (
          <li
            key={uuid.v4()}
            className={classnames('endpoint-tab', {
              active:
                endpoint.alias === (selectedHandler && selectedHandler.alias),
            })}
            onClick={handleChooseHandler(endpoint)}
          >
            {endpoint.text}
            <button
              className="endpoint-tab--delete"
              onClick={handleRemoveHandler(endpoint)}
            />
          </li>
        )
      })}
    </ul>
  ) : null

export default HandlerTabs
