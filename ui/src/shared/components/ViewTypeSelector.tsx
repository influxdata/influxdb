// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Constants
import {GRAPH_TYPES} from 'src/dashboards/graphics/graph'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {ViewType} from 'src/types/v2'

interface Props {
  type: string
  onUpdateType: (newType: ViewType) => void
}

@ErrorHandling
class ViewTypeSelector extends Component<Props> {
  public render() {
    const {type} = this.props

    return (
      <div className="graph-type-selector--container">
        <div className="graph-type-selector--grid">
          {GRAPH_TYPES.map(graphType => (
            <div
              key={graphType.type}
              className={classnames('graph-type-selector--option', {
                active: graphType.type === type,
              })}
            >
              <div onClick={this.handleSelectType(graphType.type)}>
                {graphType.graphic}
                <p>{graphType.menuOption}</p>
              </div>
            </div>
          ))}
        </div>
      </div>
    )
  }

  private handleSelectType = (newType: ViewType) => (): void => {
    this.props.onUpdateType(newType)
  }
}

export default ViewTypeSelector
