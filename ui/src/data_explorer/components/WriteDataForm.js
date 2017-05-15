import React, {Component, PropTypes} from 'react'

import DatabaseDropdown from 'shared/components/DatabaseDropdown'

class WriteDataForm extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selectedDatabase: null,
    }

    this.handleSelectDatabase = ::this.handleSelectDatabase
    this.handleStartEdit = ::this.handleStartEdit
    this.handleError = ::this.handleError
  }

  handleSelectDatabase(item) {
    this.setState({selectedDatabase: item.text})
  }

  handleStartEdit() {}

  handleError() {}

  render() {
    const {onClose} = this.props
    const {selectedDatabase} = this.state

    return (
      <div className="template-variable-manager">
        <div className="template-variable-manager--header">
          <div className="page-header__left">
            <h1 className="page-header__title">Write Data To</h1>
            <DatabaseDropdown
              onSelectDatabase={this.handleSelectDatabase}
              database={selectedDatabase}
              onStartEdit={this.handleStartEdit}
              onErrorThrown={this.handleError}
            />
          </div>
          <div className="page-header__right">
            <span className="page-header__dismiss" onClick={onClose} />
          </div>
        </div>
        <div className="template-variable-manager--body" />
      </div>
    )
  }
}

const {func} = PropTypes

WriteDataForm.propTypes = {
  onClose: func.isRequired,
}

export default WriteDataForm
