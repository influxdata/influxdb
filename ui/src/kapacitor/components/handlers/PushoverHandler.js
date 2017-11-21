import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'

const PushoverHandler = ({
  selectedHandler,
  handleModifyHandler,
  configLink,
}) => {
  return selectedHandler.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Optional Parameters</h4>
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="userKey"
            fieldDisplay="User Key"
            placeholder="Ex: the_key"
          />
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="device"
            fieldDisplay="Device: (comma separated)"
            placeholder="Ex: dv1, dv2"
          />
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="title"
            fieldDisplay="Title:"
            placeholder="Ex: Important Alert"
          />
        </div>
        <div className="optional-alert-parameters">
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="url"
            fieldDisplay="URL:"
            placeholder="Ex: https://influxdata.com"
          />
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="urlTitle"
            fieldDisplay="URL Title:"
            placeholder="Ex: InfluxData"
          />
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="sound"
            fieldDisplay="Sound:"
            placeholder="Ex: alien"
          />
        </div>
      </div>
    : <HandlerEmpty configLink={configLink} />
}

const {func, shape, string} = PropTypes

PushoverHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  configLink: string,
}

export default PushoverHandler
