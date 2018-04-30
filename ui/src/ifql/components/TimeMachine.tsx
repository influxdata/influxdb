import React, {PureComponent} from 'react'
import BodyBuilder from 'src/ifql/components/BodyBuilder'
import TimeMachineEditor from 'src/ifql/components/TimeMachineEditor'
import TimeMachineVis from 'src/ifql/components/TimeMachineVis'
import Resizer from 'src/shared/components/ResizeContainer'
import {Suggestion, OnChangeScript, FlatBody} from 'src/types/ifql'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  script: string
  suggestions: Suggestion[]
  body: Body[]
  onChangeScript: OnChangeScript
}

interface Body extends FlatBody {
  id: string
}

@ErrorHandling
class TimeMachine extends PureComponent<Props> {
  public render() {
    return (
      <Resizer
        containerClass="page-contents"
        orientation="vertical"
        divisions={this.divisions}
      />
    )
  }

  private get divisions() {
    return [
      {
        minSize: 200,
        render: () => (
          <Resizer
            containerClass="ifql-left-panel"
            orientation="horizontal"
            divisions={this.renderEditorDivisions}
          />
        ),
      },
      {
        minSize: 200,
        render: () => <TimeMachineVis blob="Visualizer" />,
      },
    ]
  }

  private get renderEditorDivisions() {
    const {script, body, suggestions, onChangeScript} = this.props

    return [
      {
        name: 'IFQL',
        render: () => (
          <TimeMachineEditor script={script} onChangeScript={onChangeScript} />
        ),
      },
      {
        name: 'Builder',
        render: () => <BodyBuilder body={body} suggestions={suggestions} />,
      },
      {
        name: 'Schema Explorer',
        render: () => <div>Explorin all yer schemas</div>,
      },
    ]
  }
}

export default TimeMachine
