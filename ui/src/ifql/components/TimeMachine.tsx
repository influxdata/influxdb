import React, {PureComponent} from 'react'
import BodyBuilder from 'src/ifql/components/BodyBuilder'
import TimeMachineEditor from 'src/ifql/components/TimeMachineEditor'
import TimeMachineVis from 'src/ifql/components/TimeMachineVis'
import Resizer from 'src/shared/components/ResizeContainer'
import {Suggestion, OnChangeScript, FlatBody} from 'src/types/ifql'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {HANDLE_VERTICAL} from 'src/shared/constants/index'

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
        {this.renderVisualization()}
        {this.renderEditor()}
      </Resizer>
    )
  }

  private renderVisualization = () => {
    return <TimeMachineVis blob="Visualizer" />
  }

  private renderEditor = () => {
    const {script, body, suggestions, onChangeScript} = this.props

    return (
      <div>
        <TimeMachineEditor script={script} onChangeScript={onChangeScript} />
        <BodyBuilder body={body} suggestions={suggestions} />
        <div>Explorin all yer schemas</div>
      </div>
    )
  }
}

export default TimeMachine
