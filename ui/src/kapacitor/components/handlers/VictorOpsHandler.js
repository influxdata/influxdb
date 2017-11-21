import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'

const VictoropsHandler = ({
  selectedHandler,
  handleModifyHandler,
  configLink,
}) => {
  return selectedHandler.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Optional Parameters</h4>
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="routingKey"
            fieldDisplay="Routing Key:"
            placeholder="Ex: routing_key"
          />
        </div>
      </div>
    : <HandlerEmpty configLink={configLink} />
}

const {func, shape, string} = PropTypes

VictoropsHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  configLink: string,
}

export default VictoropsHandler
