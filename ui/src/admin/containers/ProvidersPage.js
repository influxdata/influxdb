import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminChronografActionCreators from 'src/admin/actions/chronograf'
import {publishAutoDismissingNotification} from 'shared/dispatchers'

import ProvidersTable from 'src/admin/components/chronograf/ProvidersTable'

import {PROVIDER_MAPS} from 'src/admin/constants/dummyProviderMaps'

class ProvidersPage extends Component {
  constructor(props) {
    super(props)
  }

  componentDidMount() {
    const {links, actions: {loadOrganizationsAsync}} = this.props

    loadOrganizationsAsync(links.organizations)
  }

  handleCreateMap = () => {}

  handleUpdateMap = _updatedMap => {
    // console.log(_updatedMap)
  }

  handleDeleteMap = () => {}

  render() {
    const {organizations, providerMaps} = this.props

    return organizations
      ? <ProvidersTable
          providerMaps={PROVIDER_MAPS} // TODO: replace with providerMaps prop
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
  providerMaps: arrayOf(
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

const mapStateToProps = ({links, adminChronograf: {organizations}}) => ({
  links,
  organizations,
})

const mapDispatchToProps = dispatch => ({
  actions: bindActionCreators(adminChronografActionCreators, dispatch),
  notify: bindActionCreators(publishAutoDismissingNotification, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(ProvidersPage)
