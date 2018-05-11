import React, {SFC} from 'react'

import HandlerInput from 'src/kapacitor/components/HandlerInput'

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
  selectedHandler: object
  handleModifyHandler: (
    selectedHandler: Handler,
    fieldName: string,
    parseToArray: string
  ) => void
}

const KafkaHandler: SFC<Props> = ({selectedHandler, handleModifyHandler}) => (
  <div className="endpoint-tab-contents">
    <div className="endpoint-tab--parameters">
      <h4>Parameters for this Alert Handler</h4>
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

export default KafkaHandler
