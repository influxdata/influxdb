import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminActionCreators from 'src/admin/actions'
import DatabaseManager from 'src/admin/components/DatabaseManager'
import {publishNotification} from 'src/shared/actions/notifications';

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

  render() {
    const {databases, actions, notify} = this.props

    return (
      <DatabaseManager
        databases={databases}
        notify={notify}
        onKeyDownDatabase={this.handleKeyDownDatabase}
        onDatabaseDeleteConfirm={this.handleDatabaseDeleteConfirm}
        addDatabase={actions.addDatabase}
        onEditDatabase={actions.editDatabase}
        onCancelDatabase={actions.removeDatabase}
        onConfirmDatabase={actions.createDatabaseAsync}
        onStartDeleteDatabase={actions.startDeleteDatabase}
        onAddRetentionPolicy={actions.addRetentionPolicy}
        onEditRetentionPolicy={actions.editRetentionPolicy}
        onCreateRetentionPolicy={actions.createRetentionPolicyAsync}
        onUpdateRetentionPolicy={actions.updateRetentionPolicyAsync}
        onRemoveRetentionPolicy={actions.removeRetentionPolicy}
      />
    )
  }

  handleKeyDownDatabase(e, database) {
    const {key} = e
    const {actions} = this.props

    if (key === 'Escape') {
      actions.removeDatabase(database)
    }

    if (key === 'Enter') {
      // TODO: validate input
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
    addRetentionPolicy: func,
    loadDBsAndRPsAsync: func,
    createDatabaseAsync: func,
    createRetentionPolicyAsync: func,
    addDatabase: func,
    removeDatabase: func,
    startDeleteDatabase: func,
    updateDatabaseDeleteCode: func,
    removeDatabaseDeleteCode: func,
    editRetentionPolicy: func,
    removeRetentionPolicy: func,
  }),
  notify: func,
}

const mapStateToProps = ({admin: {databases, retentionPolicies}}) => ({
  databases,
  retentionPolicies,
})

const mapDispatchToProps = (dispatch) => ({
  actions: bindActionCreators(adminActionCreators, dispatch),
  notify: bindActionCreators(publishNotification, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(DatabaseManagerPage)
