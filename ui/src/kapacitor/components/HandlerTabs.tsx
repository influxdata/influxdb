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
      {handlersOnThisAlert.map(ep => (
        <li
          key={uuid.v4()}
          className={classnames('endpoint-tab', {
            active: ep.alias === (selectedHandler && selectedHandler.alias),
          })}
          onClick={handleChooseHandler(ep)}
        >
          {ep.text}
          <button
            className="endpoint-tab--delete"
            onClick={handleRemoveHandler(ep)}
          />
        </li>
      ))}
    </ul>
  ) : null

export default HandlerTabs
