import React, {PropTypes, Component} from 'react'

import ConfirmButtons from 'shared/components/ConfirmButtons'
import {QUERIES_TABLE} from 'src/admin/constants/tableSizing'

class QueryRow extends Component {
  constructor(props) {
    super(props)

    this.handleInitiateKill = ::this.handleInitiateKill
    this.handleFinishHim = ::this.handleFinishHim
    this.handleShowMercy = ::this.handleShowMercy

    this.state = {
      confirmingKill: false,
    }
  }

  handleInitiateKill() {
    this.setState({confirmingKill: true})
  }

  handleFinishHim() {
    this.props.onKill(this.props.query.id)
  }

  handleShowMercy() {
    this.setState({confirmingKill: false})
  }

  render() {
    const {query: {database, query, duration}} = this.props

    return (
      <tr>
        <td
          style={{width: `${QUERIES_TABLE.colDatabase}px`}}
          className="monotype"
        >
          {database}
        </td>
        <td>
          <code>
            {query}
          </code>
        </td>
        <td
          style={{width: `${QUERIES_TABLE.colRunning}px`}}
          className="monotype"
        >
          {duration}
        </td>
        <td
          style={{width: `${QUERIES_TABLE.colKillQuery}px`}}
          className="text-right"
        >
          {this.state.confirmingKill
            ? <ConfirmButtons
                onConfirm={this.handleFinishHim}
                onCancel={this.handleShowMercy}
                buttonSize="btn-xs"
              />
            : <button
                className="btn btn-xs btn-danger table--show-on-row-hover"
                onClick={this.handleInitiateKill}
              >
                Kill
              </button>}
        </td>
      </tr>
    )
  }
}

const {func, shape} = PropTypes

QueryRow.propTypes = {
  query: shape().isRequired,
  onKill: func.isRequired,
}

export default QueryRow
