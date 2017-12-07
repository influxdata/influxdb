import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'

const PagerdutyHandler = ({
  selectedHandler,
  handleModifyHandler,
  onGoToConfig,
  validationError,
}) =>
  selectedHandler.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Parameters for this Alert Handler</h4>
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="serviceKey"
            fieldDisplay="Service Key:"
            placeholder="ex: service_key"
            redacted={true}
            fieldColumns="col-md-12"
          />
        </div>
      </div>
    : <HandlerEmpty
        onGoToConfig={onGoToConfig}
        validationError={validationError}
      />

const {func, shape, string} = PropTypes

PagerdutyHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  onGoToConfig: func.isRequired,
  validationError: string.isRequired,
}

export default PagerdutyHandler
