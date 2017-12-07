import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'

const SensuHandler = ({
  selectedHandler,
  handleModifyHandler,
  onGoToConfig,
  validationError,
}) =>
  selectedHandler.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Parameters from Kapacitor Configuration</h4>
          <div className="faux-form">
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="addr"
              fieldDisplay="Address"
              placeholder=""
              disabled={true}
            />
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="source"
              fieldDisplay="Source"
              placeholder="ex: my_source"
            />
          </div>
        </div>
        <div className="endpoint-tab--parameters">
          <h4>Parameters for this Alert Handler</h4>
          <div className="faux-form">
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="handlers"
              fieldDisplay="Handlers"
              placeholder="ex: my_handlers"
              fieldColumns="col-md-12"
              parseToArray={true}
            />
          </div>
        </div>
      </div>
    : <HandlerEmpty
        onGoToConfig={onGoToConfig}
        validationError={validationError}
      />

const {func, shape, string} = PropTypes

SensuHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  onGoToConfig: func.isRequired,
  validationError: string.isRequired,
}

export default SensuHandler
