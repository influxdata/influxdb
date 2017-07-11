import React, {PropTypes} from 'react'
import OverlayTechnologies from 'shared/components/OverlayTechnologies'

const style = {
  padding: '8px',
  borderRadius: '4px',
  backgroundColor: '#202028',
  margin: '0 15%',
  top: '16px',
  position: 'relative',
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  height: 'calc(100% - 76px)',
}

const TICKscriptOverlay = ({tickscript, onClose}) => (
  <OverlayTechnologies>
    <div style={style}>
      <div style={{display: 'flex', justifyContent: 'space-around'}}>
        <h2>Generated TICKscript</h2>
        <span
          style={{cursor: 'pointer'}}
          className="icon remove"
          onClick={onClose}
        />
      </div>
      <pre
        style={{
          border: '1px solid white',
          borderRadius: '4px',
          overflow: 'auto',
          whiteSpace: 'pre',
          color: '#4ed8a0',
          fontSize: '14px',
          width: '95%',
        }}
      >
        {tickscript}
      </pre>
    </div>
  </OverlayTechnologies>
)

const {string, func} = PropTypes

TICKscriptOverlay.propTypes = {
  tickscript: string,
  onClose: func.isRequired,
}

export default TICKscriptOverlay
