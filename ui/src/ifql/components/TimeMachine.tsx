import React, {PureComponent} from 'react'
import SchemaExplorer from 'src/ifql/components/SchemaExplorer'
import BodyBuilder from 'src/ifql/components/BodyBuilder'
import TimeMachineEditor from 'src/ifql/components/TimeMachineEditor'
import TimeMachineVis from 'src/ifql/components/TimeMachineVis'
import Threesizer from 'src/shared/components/threesizer/Threesizer'
import {
  Suggestion,
  OnChangeScript,
  OnSubmitScript,
  FlatBody,
  ScriptStatus,
  ScriptResult,
} from 'src/types/ifql'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {HANDLE_VERTICAL, HANDLE_HORIZONTAL} from 'src/shared/constants'

interface Props {
  data: ScriptResult[]
  script: string
  body: Body[]
  status: ScriptStatus
  suggestions: Suggestion[]
  onChangeScript: OnChangeScript
  onSubmitScript: OnSubmitScript
  onAppendFrom: () => void
  onAppendJoin: () => void
  onAnalyze: () => void
  visStatus: ScriptStatus
}

interface Body extends FlatBody {
  id: string
}

@ErrorHandling
class TimeMachine extends PureComponent<Props> {
  public render() {
    return (
      <Threesizer
        orientation={HANDLE_HORIZONTAL}
        divisions={this.mainSplit}
        containerClass="page-contents"
      />
    )
  }

  private get mainSplit() {
    const {data, visStatus} = this.props
    return [
      {
        handleDisplay: 'none',
        menuOptions: [],
        headerButtons: [],
        render: () => (
          <Threesizer
            divisions={this.divisions}
            orientation={HANDLE_VERTICAL}
          />
        ),
      },
      {
        handlePixels: 8,
        menuOptions: [],
        headerButtons: [],
        render: () => <TimeMachineVis data={data} status={visStatus} />,
      },
    ]
  }

  private get divisions() {
    const {
      body,
      script,
      status,
      onAnalyze,
      suggestions,
      onAppendFrom,
      onAppendJoin,
      onChangeScript,
      onSubmitScript,
    } = this.props

    return [
      {
        name: 'Explore',
        headerButtons: [],
        menuOptions: [],
        render: () => <SchemaExplorer />,
      },
      {
        name: 'Script',
        headerButtons: [
          <div
            key="analyze"
            className="btn btn-default btn-xs analyze--button"
            onClick={onAnalyze}
          >
            Analyze
          </div>,
        ],
        menuOptions: [],
        render: visibility => (
          <TimeMachineEditor
            status={status}
            script={script}
            visibility={visibility}
            onChangeScript={onChangeScript}
            onSubmitScript={onSubmitScript}
          />
        ),
      },
      {
        name: 'Build',
        headerButtons: [],
        menuOptions: [],
        render: () => (
          <BodyBuilder
            body={body}
            suggestions={suggestions}
            onAppendFrom={onAppendFrom}
            onAppendJoin={onAppendJoin}
          />
        ),
      },
    ]
  }
}

export default TimeMachine
