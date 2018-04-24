import React, {PureComponent} from 'react'
import BodyBuilder from 'src/ifql/components/BodyBuilder'
import TimeMachineEditor from 'src/ifql/components/TimeMachineEditor'

import {
  FlatBody,
  Suggestion,
  OnChangeArg,
  OnDeleteFuncNode,
  OnAddNode,
} from 'src/types/ifql'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  script: string
  suggestions: Suggestion[]
  body: Body[]
  onSubmitScript: () => void
  onChangeScript: (script: string) => void
  onAddNode: OnAddNode
  onChangeArg: OnChangeArg
  onDeleteFuncNode: OnDeleteFuncNode
  onGenerateScript: () => void
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
          return (
          <BodyBuilder body={body} suggestions={suggestions} />
          ) }}
        </div>
      </div>
    )
  }
}

export default TimeMachine
