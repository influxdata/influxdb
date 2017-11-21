import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'

const TcpHandler = ({selectedHandler, handleModifyHandler}) => {
  return (
    <div className="endpoint-tab-contents">
      <div className="endpoint-tab--parameters">
        <h4>Optional Parameters</h4>
        <HandlerInput
          selectedHandler={selectedHandler}
          handleModifyHandler={handleModifyHandler}
          fieldName="address"
          fieldDisplay="address"
          placeholder="Ex: exampleendpoint.com:5678"
        />
      </div>
    </div>
  )
}

const {func, shape} = PropTypes

TcpHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
}

export default TcpHandler
