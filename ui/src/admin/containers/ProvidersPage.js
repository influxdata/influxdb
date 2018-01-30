import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminChronografActionCreators from 'src/admin/actions/chronograf'
import {publishAutoDismissingNotification} from 'shared/dispatchers'

import ProvidersTable from 'src/admin/components/chronograf/ProvidersTable'

class ProvidersPage extends Component {
  constructor(props) {
    super(props)
  }

  componentDidMount() {
    const {
      links,
      actions: {loadOrganizationsAsync, loadMappingsAsync},
    } = this.props

    loadOrganizationsAsync(links.organizations)
    loadMappingsAsync(links.mappings)
  }

  handleCreateMap = mapping => {
    this.props.actions.createMapping(mapping)
  }

  handleUpdateMap = updatedMap => {
    // update the redux store
    this.props.actions.updateMapping(updatedMap)

    // update the server
  }

  handleDeleteMap = mapping => {
    this.props.actions.deleteMapping(mapping)
  }

  render() {
    const {organizations, mappings = []} = this.props

    return organizations
      ? <ProvidersTable
          mappings={mappings}
          organizations={organizations}
          onCreateMap={this.handleCreateMap}
          onUpdateMap={this.handleUpdateMap}
          onDeleteMap={this.handleDeleteMap}
        />
      : <div className="page-spinner" />
  }
}

const {arrayOf, func, shape, string} = PropTypes

ProvidersPage.propTypes = {
  links: shape({
    organizations: string.isRequired,
  }),
  organizations: arrayOf(
    shape({
      id: string.isRequired,
      name: string.isRequired,
    })
  ),
  mappings: arrayOf(
    shape({
      id: string,
      scheme: string,
      provider: string,
      providerOrganization: string,
      redirectOrg: shape({
        id: string.isRequired,
        name: string.isRequired,
      }),
    })
  ),
  actions: shape({
    loadOrganizationsAsync: func.isRequired,
  }),
  notify: func.isRequired,
}

const mapStateToProps = ({
  links,
  adminChronograf: {organizations, mappings},
}) => ({
  links,
  organizations,
  mappings,
})

const mapDispatchToProps = dispatch => ({
  actions: bindActionCreators(adminChronografActionCreators, dispatch),
  notify: bindActionCreators(publishAutoDismissingNotification, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(ProvidersPage)
