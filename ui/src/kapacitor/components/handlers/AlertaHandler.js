import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'

const AlertaHandler = ({selectedHandler, handleModifyHandler, configLink}) => {
  return selectedHandler.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Parameters from Kapacitor Configuration</h4>
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="environment"
            fieldDisplay="Environment"
            placeholder="Ex: environment"
          />
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="origin"
            fieldDisplay="Origin"
            placeholder="Ex: origin"
          />
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="token"
            fieldDisplay="Token"
            placeholder="Ex: my_token"
            redacted={true}
          />
        </div>
        <div className="endpoint-tab--parameters">
          <h4>Optional Parameters</h4>
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="resource"
            fieldDisplay="Resource"
            placeholder="Ex: my_resource"
          />
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="event"
            fieldDisplay="Event"
            placeholder="Ex: event"
          />
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="group"
            fieldDisplay="Group"
            placeholder="Ex: group_name"
          />
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="value"
            fieldDisplay="Value"
            placeholder="Ex: value"
          />
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="service"
            fieldDisplay="Service"
            placeholder="Ex: service_name"
          />
        </div>
      </div>
    : <HandlerEmpty configLink={configLink} />
}

const {func, shape, string} = PropTypes

AlertaHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  configLink: string,
}

export default AlertaHandler
