import React, {PropTypes, Component} from 'react'
import _ from 'lodash'
import uuid from 'node-uuid'

const style = {
  position: 'absolute',
  width: 'calc(100% - 32px)',
  bottom: '8px',
  left: '16px',
  height: '30px',
}

class PersistentLegend extends Component {
  constructor(props) {
    super(props)
  }
  handleClick = i => () => {
    const visibilities = this.props.dygraph.visibility()
    this.props.dygraph.setVisibility(i, !visibilities[i])
  }

  render() {
    const labels = this.props.dygraph
      ? _.drop(this.props.dygraph.getLabels())
      : []
    return (
      <div className="persistent-legend" style={style}>
        {_.map(labels, (v, i) =>
          <div key={uuid.v4()} onClick={this.handleClick(i)}>
            {v}
          </div>
        )}
      </div>
    )
  }
}

const {func, shape} = PropTypes

PersistentLegend.propTypes = {dygraph: shape({})}

export default PersistentLegend
