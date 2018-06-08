import React, {PureComponent} from 'react'
import SchemaExplorer from 'src/flux/components/SchemaExplorer'
import BodyBuilder from 'src/flux/components/BodyBuilder'
import TimeMachineEditor from 'src/flux/components/TimeMachineEditor'
import Threesizer from 'src/shared/components/threesizer/Threesizer'
import {
  Suggestion,
  OnChangeScript,
  OnSubmitScript,
  FlatBody,
  ScriptStatus,
} from 'src/types/flux'

import {Service} from 'src/types'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {HANDLE_VERTICAL, HANDLE_HORIZONTAL} from 'src/shared/constants'

interface Props {
  service: Service
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
        orientation={HANDLE_VERTICAL}
        divisions={this.verticals}
        containerClass="page-contents"
      />
    )
  }

  private get verticals() {
    return [
      {
        handleDisplay: 'none',
        menuOptions: [],
        headerButtons: [],
        render: () => (
          <Threesizer
            divisions={this.scriptAndExplorer}
            orientation={HANDLE_HORIZONTAL}
          />
        ),
      },
      this.builder,
    ]
  }

  private get builder() {
    const {body, service, suggestions, onAppendFrom, onAppendJoin} = this.props

    return {
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
    }
  }
  private get scriptAndExplorer() {
    const {
      script,
      status,
      service,
      onAnalyze,
      suggestions,
      onChangeScript,
      onSubmitScript,
    } = this.props

    return [
      {
        name: 'Explore',
        headerButtons: [],
        menuOptions: [],
        render: () => <SchemaExplorer service={service} />,
        headerOrientation: HANDLE_VERTICAL,
      },
      {
        name: 'Script',
        headerOrientation: HANDLE_VERTICAL,
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
            suggestions={suggestions}
            onChangeScript={onChangeScript}
            onSubmitScript={onSubmitScript}
          />
        ),
      },
    ]
  }
}

export default TimeMachine
