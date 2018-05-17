import React, {SFC} from 'react'

import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'

interface Handler {
  alias: string
  enabled: boolean
  headerKey: string
  headerValue: string
  headers: {
    [key: string]: string
  }
  text: string
  type: string
  url: string
}

interface Props {
  selectedHandler: {
    enabled: boolean
  }
  handleModifyHandler: (
    selectedHandler: Handler,
    fieldName: string,
    parseToArray: string
  ) => void
  onGoToConfig: () => void
  validationError: string
}

const KafkaHandler: SFC<Props> = ({
  selectedHandler,
  handleModifyHandler,
  onGoToConfig,
  validationError,
}) => {
  if (selectedHandler.enabled) {
    let goToConfigText
    if (validationError) {
      goToConfigText = 'Exit this Rule and Edit Configuration'
    } else {
      goToConfigText = 'Save this Rule and Edit Configuration'
    }
    return (
      <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4 className="u-flex u-jc-space-between">
            Parameters for this Alert Handler
            <div className="btn btn-default btn-sm" onClick={onGoToConfig}>
              <span className="icon cog-thick" />
              {goToConfigText}
            </div>
          </h4>
          <div className="faux-form">
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="cluster"
              fieldDisplay="Cluster"
              placeholder=""
              fieldColumns="col-md-12"
            />
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="topic"
              fieldDisplay="Topic"
              placeholder=""
            />
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="template"
              fieldDisplay="Template"
              placeholder=""
            />
          </div>
        </div>
      </div>
    )
  }

  return (
    <HandlerEmpty
      onGoToConfig={onGoToConfig}
      validationError={validationError}
    />
  )
}

export default KafkaHandler
