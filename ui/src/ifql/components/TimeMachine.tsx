import React, {PureComponent} from 'react'
import BodyBuilder from 'src/ifql/components/BodyBuilder'
import TimeMachineEditor from 'src/ifql/components/TimeMachineEditor'

import {
  Suggestion,
  OnChangeScript,
  OnSubmitScript,
  FlatBody,
} from 'src/types/ifql'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  script: string
  suggestions: Suggestion[]
  body: Body[]
  onSubmitScript: OnSubmitScript
  onChangeScript: OnChangeScript
}

interface Body extends FlatBody {
  id: string
}

@ErrorHandling
class TimeMachine extends PureComponent<Props> {
  public render() {
    const {
      body,
      script,
      onChangeScript,
      onSubmitScript,
      suggestions,
    } = this.props

    return (
      <div className="time-machine-container">
        <TimeMachineEditor
          script={script}
          onChangeScript={onChangeScript}
          onSubmitScript={onSubmitScript}
        />
        <div className="expression-container">
          <BodyBuilder body={body} suggestions={suggestions} />
        </div>
      </div>
    )
  }
}

export default TimeMachine
