import React, {PureComponent} from 'react'
import SchemaExplorer from 'src/ifql/components/SchemaExplorer'
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
        topMinPixels={440}
        bottomMinPixels={200}
        orientation={HANDLE_VERTICAL}
        containerClass="page-contents"
      >
        {this.renderEditor}
        {this.renderRightSide}
      </Resizer>
    )
  }

  private get renderRightSide() {
    return (
      <Threesizer
        divisions={this.visPlusBuilder}
        orientation={HANDLE_HORIZONTAL}
      />
    )
  }

  private get visPlusBuilder() {
    const {body, suggestions} = this.props
    return [
      {
        name: 'Build',
        render: () => <BodyBuilder body={body} suggestions={suggestions} />,
      },
      {
        name: 'Visualize',
        render: () => <TimeMachineVis blob="Incredulous" />,
      },
    ]
  }

  private get renderEditor() {
    return (
      <Threesizer divisions={this.divisions} orientation={HANDLE_HORIZONTAL} />
    )
  }

  private get divisions() {
    const {script, onChangeScript} = this.props
    return [
      {
        name: 'Script',
        render: () => (
          <TimeMachineEditor script={script} onChangeScript={onChangeScript} />
        ),
      },
      {
        name: 'Explore',
        render: () => <SchemaExplorer />,
      },
      {
        name: 'Test',
        render: () => <div>im a test</div>,
      },
    ]
  }
}

export default TimeMachine
