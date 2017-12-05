import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'

const LogHandler = ({selectedHandler, handleModifyHandler}) => {
  return (
    <div className="endpoint-tab-contents">
      <div className="endpoint-tab--parameters">
        <h4>Parameters for this Alert Handler</h4>
        <div className="faux-form">
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="filePath"
            fieldDisplay="File Path for Log File:"
            placeholder="ex: /tmp/alerts.log"
            fieldColumns="col-md-12"
          />
        </div>
      </div>
    </div>
  )
}

const {func, shape} = PropTypes

LogHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
}

export default LogHandler
