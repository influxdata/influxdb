import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminActionCreators from 'src/admin/actions'
import DatabaseManager from 'src/admin/components/DatabaseManager'

class DatabaseManagerPage extends Component {
  constructor(props) {
    super(props)
    this.handleEditDatabase = ::this.handleEditDatabase
  }

  componentDidMount() {
    const {source: {links: {proxy}}, actions} = this.props

    actions.loadDBsAndRPsAsync(proxy)
  }

  handleCreateDatabase() {
    // this.props.createDatabase(database)
  }

  handleEditDatabase(updates, database) {
    this.props.actions.editDatabase(updates, database)
  }

  handleAddDatabase() {
    this.props.actions.addDatabase()
  }

  render() {
    const {databases, retentionPolicies, actions} = this.props

    return (
      <DatabaseManager
        addDatabase={actions.addDatabase}
        databases={databases}
        retentionPolicies={retentionPolicies}
        onEditDatabase={this.handleEditDatabase}
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
    addDatabase: func,
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
