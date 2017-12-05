import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'

const TcpHandler = ({selectedHandler, handleModifyHandler}) => {
  return (
    <div className="endpoint-tab-contents">
      <div className="endpoint-tab--parameters">
        <h4>Parameters for this Alert Handler</h4>
        <div className="faux-form">
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="address"
            fieldDisplay="Address"
            placeholder="ex: exampleendpoint.com:5678"
            fieldColumns="col-md-12"
          />
        </div>
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
