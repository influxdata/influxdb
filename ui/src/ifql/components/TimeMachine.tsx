import React, {PureComponent} from 'react'
import BodyBuilder from 'src/ifql/components/BodyBuilder'
import TimeMachineEditor from 'src/ifql/components/TimeMachineEditor'
import TimeMachineVis from 'src/ifql/components/TimeMachineVis'
import ResizeContainer from 'src/shared/components/ResizeContainer'
import {MINIMUM_HEIGHTS, INITIAL_HEIGHTS} from 'src/data_explorer/constants'
import {
  Suggestion,
  OnChangeScript,
  OnSubmitScript,
  FlatBody,
} from 'src/types/ifql'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  script: string
  suggestions: Suggestion[]
  body: Body[]
  onSubmitScript: OnSubmitScript
  onChangeScript: OnChangeScript
}

interface Body extends FlatBody {
  id: string
}

@ErrorHandling
class TimeMachine extends PureComponent<Props> {
  public render() {
    return (
      <ResizeContainer
        containerClass="page-contents"
        minTopHeight={MINIMUM_HEIGHTS.queryMaker}
        minBottomHeight={MINIMUM_HEIGHTS.visualization}
        initialTopHeight={INITIAL_HEIGHTS.queryMaker}
        initialBottomHeight={INITIAL_HEIGHTS.visualization}
        renderTop={this.renderTop}
        renderBottom={this.renderBottom}
      />
    )
  }

  private renderTop = height => {
    const {
      body,
      script,
      onChangeScript,
      onSubmitScript,
      suggestions,
    } = this.props
    return (
      <div style={{height}}>
        <TimeMachineEditor
          script={script}
          onChangeScript={onChangeScript}
          onSubmitScript={onSubmitScript}
        />
        <BodyBuilder body={body} suggestions={suggestions} />
      </div>
    )
  }

  private renderBottom = (top, height) => (
    <TimeMachineVis bottomHeight={height} topHeight={top} />
  )
}

export default TimeMachine
