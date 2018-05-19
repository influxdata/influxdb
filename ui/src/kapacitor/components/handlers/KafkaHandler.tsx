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
  id: number
}

interface Props {
  selectedHandler: Handler
  handleModifyHandler: (
    selectedHandler: Handler,
    fieldName: string,
    parseToArray: boolean,
    headerIndex: number
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
  const handler = {...selectedHandler, cluster: selectedHandler.id}
  handleModifyHandler(handler, 'cluster', false, 0)

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
              selectedHandler={handler}
              handleModifyHandler={handleModifyHandler}
              fieldName="topic"
              fieldDisplay="Topic"
              placeholder=""
            />
            <HandlerInput
              selectedHandler={handler}
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
