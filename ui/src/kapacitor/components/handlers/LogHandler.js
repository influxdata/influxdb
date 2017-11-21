import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'

const LogHandler = ({selectedHandler, handleModifyHandler}) => {
  return (
    <div className="endpoint-tab-contents">
      <div className="endpoint-tab--parameters">
        <h4>Optional Parameters</h4>
        <HandlerInput
          selectedHandler={selectedHandler}
          handleModifyHandler={handleModifyHandler}
          fieldName="filePath"
          fieldDisplay="Log File Path:"
          placeholder="Ex: /tmp/alerts.log"
        />
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
