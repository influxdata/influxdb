import React, {Component, PropTypes} from 'react'

import ProviderMap from 'src/admin/components/chronograf/ProviderMap'
// import NewProviderMap from 'src/admin/components/chronograf/NewProviderMap'

class ProvidersTable extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isCreatingMap: false,
    }
  }
  handleClickCreateMap = () => {
    this.setState({isCreatingMap: true})
  }

  handleDismissCreateMap = () => {
    this.setState({isCreatingMap: false})
  }

  handleCreateMap = newMap => {
    const {onCreateMap} = this.props
    onCreateMap(newMap)
  }

  render() {
    const {providerMaps, organizations, onUpdateMap, onDeleteMap} = this.props
    const {isCreatingMap} = this.state

    const tableTitle =
      providerMaps.length === 1 ? '1 Map' : `${providerMaps.length} Maps`

    return (
      <div className="panel panel-default">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h2 className="panel-title">
            {tableTitle}
          </h2>
          <button
            className="btn btn-sm btn-primary"
            onClick={this.handleClickCreateMap}
            disabled={isCreatingMap}
          >
            <span className="icon plus" /> Create Map
          </button>
        </div>
        <div className="panel-body">
          <div className="providers-labels">
            <div className="providers-labels--id">ID</div>
            <div className="providers-labels--scheme">Scheme</div>
            <div className="providers-labels--provider">Provider</div>
            <div className="providers-labels--providerorg">Provider Org</div>
            <div className="providers-labels--arrow" />
            <div className="providers-labels--redirect">Organization</div>
            <div className="providers-labels--delete" />
          </div>
          {/* {isCreatingMap
                  ? <NewProviderMap
                      onCreateMap={this.handleCreateMap}
                      onDismissCreateMap={this.handleDismissCreateMap}
                    />
                  : null} */}
          {providerMaps.map(providerMap =>
            <ProviderMap
              key={providerMap.id}
              providerMap={providerMap}
              organizations={organizations}
              onDelete={onDeleteMap}
              onUpdate={onUpdateMap}
            />
          )}
        </div>
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

ProvidersTable.propTypes = {
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
  ).isRequired,
  organizations: arrayOf(
    shape({
      id: string, // when optimistically created, organization will not have an id
      name: string.isRequired,
    })
  ).isRequired,
  onCreateMap: func.isRequired,
  onUpdateMap: func.isRequired,
  onDeleteMap: func.isRequired,
}
export default ProvidersTable
