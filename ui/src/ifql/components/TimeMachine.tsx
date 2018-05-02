import React, {PureComponent} from 'react'
import BodyBuilder from 'src/ifql/components/BodyBuilder'
import TimeMachineEditor from 'src/ifql/components/TimeMachineEditor'
import TimeMachineVis from 'src/ifql/components/TimeMachineVis'
import Resizer from 'src/shared/components/ResizeContainer'
import Threesizer from 'src/shared/components/Threesizer'
import {Suggestion, OnChangeScript, FlatBody} from 'src/types/ifql'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {HANDLE_VERTICAL, HANDLE_HORIZONTAL} from 'src/shared/constants/index'

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
        topMinPixels={200}
        bottomMinPixels={200}
        orientation={HANDLE_VERTICAL}
        containerClass="page-contents"
      >
        {this.renderEditor}
        {this.renderVisualization}
      </Resizer>
    )
  }

  private get renderVisualization() {
    return <TimeMachineVis blob="Visualizer" />
  }

  private get renderEditor() {
    return (
      <Threesizer divisions={this.divisions} orientation={HANDLE_HORIZONTAL} />
    )
  }

  private get divisions() {
    const {script, body, suggestions, onChangeScript} = this.props
    return [
      {
        name: 'Editor',
        render: () => (
          <TimeMachineEditor script={script} onChangeScript={onChangeScript} />
        ),
      },
      {
        name: 'Builder',
        render: () => <BodyBuilder body={body} suggestions={suggestions} />,
      },
      {
        name: 'Schema',
        render: () => <div>Explorin all yer schemas</div>,
      },
    ]
  }
}

export default TimeMachine
