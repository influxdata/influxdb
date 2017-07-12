import React, {PropTypes} from 'react'
import OverlayTechnologies from 'shared/components/OverlayTechnologies'

const TICKscriptOverlay = ({tickscript, onClose}) =>
  <OverlayTechnologies>
    <div className="tick-script-overlay">
      <div className="write-data-form--header">
        <div className="page-header__left">
          <h1 className="page-header__title">Generated TICKscript</h1>
        </div>
        <div className="page-header__right">
          <span className="page-header__dismiss" onClick={onClose} />
        </div>
      </div>
      <div className="write-data-form--body">
        <pre className="tick-script-overlay--sample">
          {tickscript}
        </pre>
      </div>
    </div>
  </OverlayTechnologies>

const {string, func} = PropTypes

TICKscriptOverlay.propTypes = {
  tickscript: string,
  onClose: func.isRequired,
}

export default TICKscriptOverlay
