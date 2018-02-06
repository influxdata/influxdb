import React, {PropTypes} from 'react'

const TickscriptEditorConsole = ({consoleMessage, unsavedChanges}) => {
  let consoleOutput = 'TICKscript is valid'
  let consoleClass = 'tickscript-console--valid'

  if (consoleMessage) {
    consoleOutput = consoleMessage
    consoleClass = 'tickscript-console--error'
  } else if (unsavedChanges) {
    consoleOutput = 'You have unsaved changes, save to validate TICKscript'
    consoleClass = 'tickscript-console--default'
  }

  return (
    <div className="tickscript-console">
      <p className={consoleClass}>
        {consoleOutput}
      </p>
    </div>
  )
}

const {bool, string} = PropTypes

TickscriptEditorConsole.propTypes = {
  consoleMessage: string,
  unsavedChanges: bool,
}

export default TickscriptEditorConsole
