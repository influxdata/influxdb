import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerCheckbox from 'src/kapacitor/components/HandlerCheckbox'

const HttpHandler = ({selectedHandler, handleModifyHandler}) => {
  return (
    <div className="endpoint-tab-contents">
      <div className="endpoint-tab--parameters">
        <h4>Optional Parameters</h4>
        <HandlerInput
          selectedHandler={selectedHandler}
          handleModifyHandler={handleModifyHandler}
          fieldName="url"
          fieldDisplay="POST URL"
          placeholder="Ex: http://example.com/api/alert"
        />
        <HandlerCheckbox
          selectedHandler={selectedHandler}
          handleModifyHandler={handleModifyHandler}
          fieldName="captureResponse"
          fieldDisplay="Capture response"
        />
      </div>
    </div>
  )
}

const {func, shape} = PropTypes

HttpHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
}

export default HttpHandler
