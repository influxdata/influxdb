import React, {PureComponent} from 'react'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  data: string
}

@ErrorHandling
class TimeMachineVis extends PureComponent<Props> {
  public render() {
    return (
      <div className="time-machine-visualization">
        <div className="time-machine--graph">
          <FancyScrollbar>
            <div className="time-machine--graph-body">
              {this.data.map((d, i) => {
                return (
                  <div key={i} className="data-row">
                    {d}
                  </div>
                )
              })}
            </div>
          </FancyScrollbar>
        </div>
      </div>
    )
  }

  private get data(): string[] {
    const {data} = this.props
    if (!data) {
      return ['Your query was syntactically correct but returned no data']
    }

    return this.props.data.split('\n')
  }
}

export default TimeMachineVis
