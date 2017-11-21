import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'

const SmtpHandler = ({selectedHandler, handleModifyHandler, configLink}) => {
  return selectedHandler.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Optional Parameters</h4>
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="to"
            fieldDisplay="E-mail Addresses: (separated by spaces)"
            placeholder="Ex: bob@domain.com susan@domain.com"
          />
        </div>
      </div>
    : <HandlerEmpty configLink={configLink} />
}

const {func, shape, string} = PropTypes

SmtpHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  configLink: string,
}

export default SmtpHandler
