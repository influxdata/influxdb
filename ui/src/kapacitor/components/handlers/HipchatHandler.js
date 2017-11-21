import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'
// import RedactedInput from './RedactedInput'

// <RedactedInput
//   defaultValue={token}
//   id="token"
//   refFunc={this.handleTokenRef}
// />
const HipchatHandler = ({selectedHandler, handleModifyHandler, configLink}) => {
  return selectedHandler.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Parameters:</h4>
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="url"
            fieldDisplay="Subdomain Url"
            placeholder="Ex: hipchat_subdomain"
            editable={false}
          />
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="room"
            fieldDisplay="Room:"
            placeholder="Ex: room_name"
          />
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="token"
            fieldDisplay="Token:"
            placeholder="Ex: the_token"
            redacted={true}
          />
        </div>
      </div>
    : <HandlerEmpty configLink={configLink} />
}

const {func, shape, string} = PropTypes

HipchatHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  configLink: string,
}

export default HipchatHandler
