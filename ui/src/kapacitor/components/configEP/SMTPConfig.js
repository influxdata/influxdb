import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'

const SmtpConfig = ({selectedEndpoint, handleModifyEndpoint}) => {
  return (
    <div className="endpoint-tab-contents">
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
  )
}

const {func, shape} = PropTypes

SmtpConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default SmtpConfig
