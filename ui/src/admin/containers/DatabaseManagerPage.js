import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminActionCreators from 'src/admin/actions'
import DatabaseManager from 'src/admin/components/DatabaseManager'

class DatabaseManagerPage extends Component {
  constructor(props) {
    super(props)
    this.handleEditDatabase = ::this.handleEditDatabase
    this.handleKeyDownDatabase = ::this.handleKeyDownDatabase
  }

  componentDidMount() {
    const {source: {links: {proxy}}, actions} = this.props

    actions.loadDBsAndRPsAsync(proxy)
  }

  handleEditDatabase(updates, database) {
    this.props.actions.editDatabase(updates, database)
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

  render() {
    const {databases, retentionPolicies, actions} = this.props

    return (
      <DatabaseManager
        addDatabase={actions.addDatabase}
        databases={databases}
        retentionPolicies={retentionPolicies}
        onEditDatabase={this.handleEditDatabase}
        onKeyDownDatabase={this.handleKeyDownDatabase}
        onCancelDatabase={actions.removeDatabase}
        onConfirmDatabase={actions.createDatabaseAsync}
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
