import React, {PropTypes, Component} from 'react'

import ConfirmButtons from 'src/shared/components/ConfirmButtons'

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
        <td>{database}</td>
        <td><code>{query}</code></td>
        <td>{duration}</td>
        <td className="admin-table--kill-button text-right">
          { this.state.confirmingKill ?
            <ConfirmButtons onConfirm={this.handleFinishHim} onCancel={this.handleShowMercy} /> :
            <button className="btn btn-xs btn-danger admin-table--hidden" onClick={this.handleInitiateKill}>Kill</button>
          }
        </td>
      </tr>
    )
  }
}

const {
  func,
  shape,
} = PropTypes

QueryRow.propTypes = {
  query: shape().isRequired,
  onKill: func.isRequired,
}

export default QueryRow
