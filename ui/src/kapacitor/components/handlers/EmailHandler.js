import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'
import RuleDetailsText from 'src/kapacitor/components/RuleDetailsText'

const EmailHandler = ({
  rule,
  updateDetails,
  selectedHandler,
  handleModifyHandler,
  onGoToConfig,
  validationError,
}) =>
  selectedHandler.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4 className="u-flex u-jc-space-between">
            Parameters from Kapacitor Configuration
            <div className="btn btn-default btn-sm" onClick={onGoToConfig}>
              <span className="icon cog-thick" />
              Edit Kapacitor Configuration
            </div>
          </h4>
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
    : <HandlerEmpty
        onGoToConfig={onGoToConfig}
        validationError={validationError}
      />

const {func, shape, string} = PropTypes

EmailHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  updateDetails: func,
  rule: shape({}),
  onGoToConfig: func.isRequired,
  validationError: string.isRequired,
}

export default EmailHandler
