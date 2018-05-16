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
  Status,
} from 'src/types/ifql'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {HANDLE_VERTICAL, HANDLE_HORIZONTAL} from 'src/shared/constants'

interface Props {
  data: string
  script: string
  body: Body[]
  status: Status
  suggestions: Suggestion[]
  onChangeScript: OnChangeScript
  onSubmitScript: OnSubmitScript
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
        render: () => <TimeMachineVis data={data} />,
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
      onChangeScript,
      onSubmitScript,
    } = this.props

    return [
      {
        name: 'Explore',
        headerButtons: [
          <div key="analyze" className="btn btn-primary btn-xs">
            Analyze
          </div>,
        ],
        menuOptions: [],
        render: () => <SchemaExplorer />,
      },
      {
        name: 'Script',
        headerButtons: [
          <div
            key="analyze"
            className="btn btn-default btn-sm analyze--button"
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
        render: () => <BodyBuilder body={body} suggestions={suggestions} />,
      },
    ]
  }
}

export default TimeMachine
