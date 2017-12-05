import React, {PropTypes} from 'react'

const TickscriptEditorConsole = ({validation}) =>
  <div className="tickscript-console">
    <div className="tickscript-console--output">
      {validation
        ? <p>
            {validation}
          </p>
        : <p className="tickscript-console--default">
            Save your TICKscript to validate it
          </p>}
    </div>
  </div>

const {string} = PropTypes

TickscriptEditorConsole.propTypes = {
  validation: string,
}

export default TickscriptEditorConsole
