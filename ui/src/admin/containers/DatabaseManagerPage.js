import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminActionCreators from 'src/admin/actions'
import DatabaseManager from 'src/admin/components/DatabaseManager'

class DatabaseManagerPage extends Component {
  constructor(props) {
    super(props)
    this.handleKeyDownDatabase = ::this.handleKeyDownDatabase
    this.handleDatabaseDeleteConfirm = ::this.handleDatabaseDeleteConfirm
  }

  componentDidMount() {
    const {source: {links: {proxy}}, actions} = this.props

    actions.loadDBsAndRPsAsync(proxy)
  }

  handleKeyDownDatabase(e, database) {
    const {key} = e
    const {actions} = this.props

    if (key === 'Escape') {
      actions.removeDatabase(database)
    }

    if (key === 'Enter') {
      actions.createDatabaseAsync(database)
    }
  }

  handleDatabaseDeleteConfirm(database, e) {
    const {key, target: {value}} = e
    const {actions, source} = this.props

    if (key === 'Escape') {
      return actions.removeDatabaseDeleteCode(database)
    }

    if (key === 'Enter' && database.deleteCode === 'DELETE') {
      return actions.deleteDatabaseAsync(source, database)
    }

    actions.updateDatabaseDeleteCode(database, value)
  }

  render() {
    const {databases, retentionPolicies, actions} = this.props

    return (
      <DatabaseManager
        databases={databases}
        retentionPolicies={retentionPolicies}
        onKeyDownDatabase={this.handleKeyDownDatabase}
        onDatabaseDeleteConfirm={this.handleDatabaseDeleteConfirm}
        addDatabase={actions.addDatabase}
        onEditDatabase={actions.editDatabase}
        onCancelDatabase={actions.removeDatabase}
        onConfirmDatabase={actions.createDatabaseAsync}
        onStartDeleteDatabase={actions.startDeleteDatabase}
      />
    )
  }
}

const {
  arrayOf,
  bool,
  func,
  number,
  shape,
  string,
} = PropTypes

DatabaseManagerPage.propTypes = {
  source: shape({
    links: shape({
      proxy: string,
    }),
  }),
  databases: arrayOf(shape({
    name: string,
    isEditing: bool,
  })),
  retentionPolicies: arrayOf(arrayOf(shape({
    name: string,
    duration: string,
    replication: number,
    isDefault: bool,
  }))),
  actions: shape({
    loadDBsAndRPsAsync: func,
    createDatabaseAsync: func,
    addDatabase: func,
    removeDatabase: func,
    startDeleteDatabase: func,
    updateDatabaseDeleteCode: func,
    removeDatabaseDeleteCode: func,
  }),
}

const mapStateToProps = ({admin: {databases, retentionPolicies}}) => ({
  databases,
  retentionPolicies,
})

const mapDispatchToProps = (dispatch) => ({
  actions: bindActionCreators(adminActionCreators, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(DatabaseManagerPage)
