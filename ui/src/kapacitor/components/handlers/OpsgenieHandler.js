import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'

const OpsgenieHandler = ({
  selectedHandler,
  handleModifyHandler,
  configLink,
}) => {
  return selectedHandler.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Parameters from Kapacitor Configuration</h4>
          <div className="faux-form">
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="api-key"
              fieldDisplay="API-key"
              placeholder=""
              redacted={true}
              disabled={true}
              fieldColumns="col-md-12"
            />
          </div>
        </div>
        <div className="endpoint-tab--parameters">
          <h4>Parameters for this Alert Handler:</h4>
          <div className="faux-form">
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="teams"
              fieldDisplay="Teams"
              placeholder="ex: teams_name"
              parseToArray={true}
            />
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="recipients"
              fieldDisplay="Recipients"
              placeholder="ex: recipients_name"
              parseToArray={true}
            />
          </div>
        </div>
      </div>
    : <HandlerEmpty configLink={configLink} />
}

const {func, shape, string} = PropTypes

OpsgenieHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  configLink: string,
}

export default OpsgenieHandler
