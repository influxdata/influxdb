import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import _ from 'lodash'

import DatabaseManager from 'src/admin/components/DatabaseManager'

import * as adminActionCreators from 'src/admin/actions/influxdb'
import {notify as notifyAction} from 'shared/actions/notifications'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {
  notifyDatabaseDeleteConfirmationRequired,
  notifyDatabaseNameAlreadyExists,
  notifyDatabaseNameInvalid,
} from 'shared/copy/notifications'

@ErrorHandling
class DatabaseManagerPage extends Component {
  constructor(props) {
    super(props)
  }

  componentDidMount() {
    const {
      source: {
        links: {databases},
      },
      actions,
    } = this.props

    actions.loadDBsAndRPsAsync(databases)
  }

  handleDeleteRetentionPolicy = (db, rp) => () => {
    this.props.actions.deleteRetentionPolicyAsync(db, rp)
  }

  handleStartDeleteDatabase = database => () => {
    this.props.actions.addDatabaseDeleteCode(database)
  }

  handleEditDatabase = database => e => {
    this.props.actions.editDatabase(database, {name: e.target.value})
  }

  handleCreateDatabase = database => {
    const {actions, notify, source, databases} = this.props
    if (!database.name) {
      return notify(notifyDatabaseNameInvalid())
    }

    if (_.findIndex(databases, {name: database.name}, 1) !== -1) {
      return notify(notifyDatabaseNameAlreadyExists())
    }

    actions.createDatabaseAsync(source.links.databases, database)
  }

  handleAddRetentionPolicy = database => () => {
    const {addRetentionPolicy} = this.props.actions
    addRetentionPolicy(database)
  }

  handleKeyDownDatabase = database => e => {
    const {key} = e
    const {actions, notify, source, databases} = this.props

    if (key === 'Escape') {
      actions.removeDatabase(database)
    }

    if (key === 'Enter') {
      if (!database.name) {
        return notify(notifyDatabaseNameInvalid())
      }

      if (_.findIndex(databases, {name: database.name}, 1) !== -1) {
        return notify(notifyDatabaseNameAlreadyExists())
      }

      actions.createDatabaseAsync(source.links.databases, database)
    }
  }

  handleDatabaseDeleteConfirm = database => e => {
    const {
      key,
      target: {value},
    } = e
    const {actions, notify} = this.props

    if (key === 'Escape') {
      return actions.removeDatabaseDeleteCode(database)
    }

    if (key === 'Enter') {
      if (database.deleteCode !== `DELETE ${database.name}`) {
        return notify(notifyDatabaseDeleteConfirmationRequired(database.name))
      }

      return actions.deleteDatabaseAsync(database)
    }

    actions.editDatabase(database, {deleteCode: value})
  }

  render() {
    const {source, databases, actions, notify} = this.props
    return (
      <DatabaseManager
        notify={notify}
        databases={databases}
        isRFDisplayed={!!source.metaUrl}
        addDatabase={actions.addDatabase}
        onEditDatabase={this.handleEditDatabase}
        onCancelDatabase={actions.removeDatabase}
        onConfirmDatabase={this.handleCreateDatabase}
        onDeleteDatabase={actions.deleteDatabaseAsync}
        onKeyDownDatabase={this.handleKeyDownDatabase}
        onAddRetentionPolicy={this.handleAddRetentionPolicy}
        onRemoveDeleteCode={actions.removeDatabaseDeleteCode}
        onStartDeleteDatabase={this.handleStartDeleteDatabase}
        isAddDBDisabled={!!databases.some(db => db.isEditing)}
        onRemoveRetentionPolicy={actions.removeRetentionPolicy}
        onDeleteRetentionPolicy={this.handleDeleteRetentionPolicy}
        onDatabaseDeleteConfirm={this.handleDatabaseDeleteConfirm}
        onCreateRetentionPolicy={actions.createRetentionPolicyAsync}
        onUpdateRetentionPolicy={actions.updateRetentionPolicyAsync}
      />
    )
  }
}

const {arrayOf, bool, func, number, shape, string} = PropTypes

DatabaseManagerPage.propTypes = {
  source: shape({
    links: shape({
      proxy: string,
    }),
  }),
  databases: arrayOf(
    shape({
      name: string,
      isEditing: bool,
    })
  ),
  retentionPolicies: arrayOf(
    arrayOf(
      shape({
        name: string,
        duration: string,
        replication: number,
        isDefault: bool,
      })
    )
  ),
  actions: shape({
    addRetentionPolicy: func,
    loadDBsAndRPsAsync: func,
    createDatabaseAsync: func,
    createRetentionPolicyAsync: func,
    addDatabase: func,
    removeDatabase: func,
    startDeleteDatabase: func,
    removeDatabaseDeleteCode: func,
    removeRetentionPolicy: func,
    deleteRetentionPolicyAsync: func,
  }),
  notify: func.isRequired,
}

const mapStateToProps = ({adminInfluxDB: {databases, retentionPolicies}}) => ({
  databases,
  retentionPolicies,
})

const mapDispatchToProps = dispatch => ({
  actions: bindActionCreators(adminActionCreators, dispatch),
  notify: bindActionCreators(notifyAction, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(DatabaseManagerPage)
