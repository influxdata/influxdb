import React, {PureComponent, CSSProperties} from 'react'
import SchemaExplorer from 'src/flux/components/SchemaExplorer'
import BodyBuilder from 'src/flux/components/BodyBuilder'
import TimeMachineEditor from 'src/flux/components/TimeMachineEditor'
import TimeMachineVis from 'src/flux/components/TimeMachineVis'
import Threesizer from 'src/shared/components/threesizer/Threesizer'
import {
  Suggestion,
  OnChangeScript,
  OnSubmitScript,
  FlatBody,
  ScriptStatus,
  FluxTable,
} from 'src/types/flux'

import {Service} from 'src/types'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {HANDLE_VERTICAL, HANDLE_HORIZONTAL} from 'src/shared/constants'

interface Props {
  service: Service
  data: FluxTable[]
  script: string
  body: Body[]
  status: ScriptStatus
  suggestions: Suggestion[]
  onChangeScript: OnChangeScript
  onSubmitScript: OnSubmitScript
  onAppendFrom: () => void
  onAppendJoin: () => void
  onAnalyze: () => void
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
    const {data} = this.props
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
        style: {overflow: 'visible'} as CSSProperties,
        render: () => <TimeMachineVis data={data} />,
      },
    ]
  }

  private get divisions() {
    const {
      body,
      script,
      status,
      service,
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
        render: () => <SchemaExplorer service={service} />,
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
            service={service}
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
