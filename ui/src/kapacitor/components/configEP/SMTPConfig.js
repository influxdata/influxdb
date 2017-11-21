import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'
import EmptyEndpoint from 'src/kapacitor/components/EmptyEndpoint'

const SmtpConfig = ({selectedEndpoint, handleModifyEndpoint, configLink}) => {
  return selectedEndpoint.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Optional Parameters</h4>
          <EndpointInput
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            fieldName="to"
            fieldDisplay="E-mail Addresses: (separated by spaces)"
            placeholder="Ex: bob@domain.com susan@domain.com"
          />
        </div>
      </div>
    : <EmptyEndpoint configLink={configLink} />
}

const {func, shape, string} = PropTypes

SmtpConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
  configLink: string,
}

export default SmtpConfig
