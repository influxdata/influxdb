import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'

const AlertaHandler = ({selectedHandler, handleModifyHandler, configLink}) =>
  selectedHandler.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Parameters from Kapacitor Configuration</h4>
          <div className="faux-form">
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="token"
              fieldDisplay="Token"
              placeholder="ex: my_token"
              redacted={true}
              fieldColumns="col-md-12"
            />
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="environment"
              fieldDisplay="Environment"
              placeholder="ex: environment"
            />

            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="origin"
              fieldDisplay="Origin"
              placeholder="ex: origin"
            />
          </div>
        </div>
        <div className="endpoint-tab--parameters">
          <h4>Parameters for this Alert Handler</h4>
          <div className="faux-form">
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="resource"
              fieldDisplay="Resource"
              placeholder=""
            />
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="event"
              fieldDisplay="Event"
              placeholder=""
            />
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="group"
              fieldDisplay="Group"
              placeholder=""
            />
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="value"
              fieldDisplay="Value"
              placeholder=""
            />
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="service"
              fieldDisplay="Service"
              placeholder=""
              parseToArray={true}
            />
          </div>
        </div>
      </div>
    : <HandlerEmpty configLink={configLink} />

const {func, shape, string} = PropTypes

AlertaHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  configLink: string,
}

export default AlertaHandler
