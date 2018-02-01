import React, {PropTypes} from 'react'

const TickscriptEditorConsole = ({consoleMessage, unsavedChanges}) => {
  let consoleOutput
  let consoleClass = 'tickscript-console--default'

  if (!unsavedChanges) {
    consoleOutput = 'TICKscript is valid'
    consoleClass = 'tickscript-console--valid'
  } else if (unsavedChanges && !consoleMessage) {
    consoleOutput = 'You have unsaved changes, save to validate TICKscript'
    consoleClass = 'tickscript-console--default'
  } else if (unsavedChanges && consoleMessage) {
    consoleOutput = consoleMessage
    consoleClass = 'tickscript-console--error'
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
