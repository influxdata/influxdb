import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'
import RuleDetailsText from 'src/kapacitor/components/RuleDetailsText'

const EmailHandler = ({
  rule,
  updateDetails,
  selectedHandler,
  handleModifyHandler,
  configLink,
}) =>
  selectedHandler.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Parameters from Kapacitor Configuration</h4>
          <div className="faux-form">
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="from"
              fieldDisplay="From E-mail"
              placeholder=""
              disabled={true}
              fieldColumns="col-md-4"
            />
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="host"
              fieldDisplay="SMTP Host"
              placeholder=""
              disabled={true}
              fieldColumns="col-md-4"
            />
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="port"
              fieldDisplay="SMTP Port"
              placeholder=""
              disabled={true}
              fieldColumns="col-md-4"
            />
          </div>
        </div>
        <div className="endpoint-tab--parameters">
          <h4>Parameters for this Alert Handler</h4>
          <div className="faux-form">
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="to"
              fieldDisplay="Recipient E-mail Addresses: (separated by spaces)"
              placeholder="ex: bob@domain.com susan@domain.com"
              parseToArray={true}
              fieldColumns="col-md-12"
            />
            <RuleDetailsText rule={rule} updateDetails={updateDetails} />
          </div>
        </div>
      </div>
    : <HandlerEmpty configLink={configLink} />

const {func, shape, string} = PropTypes

EmailHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  configLink: string,
  updateDetails: func,
  rule: shape({}),
}

export default EmailHandler
