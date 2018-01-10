import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'

const ExecHandler = ({selectedHandler, handleModifyHandler}) =>
  <div className="endpoint-tab-contents">
    <div className="endpoint-tab--parameters">
      <h4>Parameters for this Alert Handler</h4>
      <div className="faux-form">
        <HandlerInput
          selectedHandler={selectedHandler}
          handleModifyHandler={handleModifyHandler}
          fieldName="command"
          fieldDisplay="Command (arguments separated by spaces):"
          placeholder="ex: command argument"
          fieldColumns="col-md-12"
          parseToArray={true}
        />
      </div>
    </div>
  </div>

const {func, shape} = PropTypes

ExecHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
}

export default ExecHandler
