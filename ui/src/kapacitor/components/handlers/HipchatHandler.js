import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'

const HipchatHandler = ({selectedHandler, handleModifyHandler, configLink}) =>
  selectedHandler.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Parameters for this Alert Handler</h4>
          <div className="faux-form">
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="token"
              fieldDisplay="Token:"
              placeholder="ex: the_token"
              redacted={true}
              fieldColumns="col-md-12"
            />
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="url"
              fieldDisplay="Subdomain Url"
              placeholder="ex: hipchat_subdomain"
              disabled={true}
            />
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="room"
              fieldDisplay="Room:"
              placeholder="ex: room_name"
            />
          </div>
        </div>
      </div>
    : <HandlerEmpty configLink={configLink} />

const {func, shape, string} = PropTypes

HipchatHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  configLink: string,
}

export default HipchatHandler
