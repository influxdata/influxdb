import React, {PureComponent} from 'react'
import SchemaExplorer from 'src/flux/components/SchemaExplorer'
import BodyBuilder from 'src/flux/components/BodyBuilder'
import TimeMachineEditor from 'src/flux/components/TimeMachineEditor'
import Threesizer from 'src/shared/components/threesizer/Threesizer'
import {
  Suggestion,
  OnChangeScript,
  OnSubmitScript,
  OnDeleteBody,
  FlatBody,
  ScriptStatus,
} from 'src/types/flux'

import {Source} from 'src/types/v2'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {HANDLE_VERTICAL, HANDLE_HORIZONTAL} from 'src/shared/constants'

interface Props {
  source: Source
  script: string
  body: Body[]
  status: ScriptStatus
  suggestions: Suggestion[]
  onChangeScript: OnChangeScript
  onDeleteBody: OnDeleteBody
  onSubmitScript: OnSubmitScript
  onAppendFrom: () => void
  onAppendJoin: () => void
  onValidate: () => void
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
        size: 0.33,
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
    const {
      body,
      suggestions,
      onAppendFrom,
      onDeleteBody,
      onAppendJoin,
    } = this.props

    return {
      name: 'Build',
      headerButtons: [],
      menuOptions: [],
      size: 0.67,
      render: () => (
        <BodyBuilder
          body={body}
          suggestions={suggestions}
          onDeleteBody={onDeleteBody}
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
      source,
      onValidate,
      suggestions,
      onChangeScript,
      onSubmitScript,
    } = this.props

    return [
      {
        name: 'Script',
        handlePixels: 44,
        headerOrientation: HANDLE_VERTICAL,
        headerButtons: [
          <div
            key="validate"
            className="btn btn-default btn-xs validate--button"
            onClick={onValidate}
          >
            Validate
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
      {
        name: 'Explore',
        handlePixels: 44,
        headerButtons: [],
        menuOptions: [],
        render: () => <SchemaExplorer source={source} />,
        headerOrientation: HANDLE_VERTICAL,
      },
    ]
  }
}

export default TimeMachine
