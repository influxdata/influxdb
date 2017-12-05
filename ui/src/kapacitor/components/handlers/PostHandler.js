import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'

const HttpHandler = ({selectedHandler, handleModifyHandler}) => {
  return (
    <div className="endpoint-tab-contents">
      <div className="endpoint-tab--parameters">
        <h4>Parameters for this Alert Handler</h4>
        <div className="faux-form">
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="url"
            fieldDisplay="HTTP endpoint for POST request"
            placeholder="ex: http://example.com/api/alert"
            fieldColumns="col-md-12"
          />
        </div>
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
