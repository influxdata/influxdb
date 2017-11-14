import React, {Component, PropTypes} from 'react'

import ProvidersTableRow from 'src/admin/components/chronograf/ProvidersTableRow'
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
          <div className="fancytable--labels">
            <div className="fancytable--th provider--id">ID</div>
            <div className="fancytable--th provider--scheme">Scheme</div>
            <div className="fancytable--th provider--provider">Provider</div>
            <div className="fancytable--th provider--providerorg">
              Provider Org
            </div>
            <div className="fancytable--th provider--arrow" />
            <div className="fancytable--th provider--redirect">
              Organization
            </div>
            <div className="fancytable--th provider--delete" />
          </div>
          {/* {isCreatingMap
                  ? <NewProviderMap
                      onCreateMap={this.handleCreateMap}
                      onDismissCreateMap={this.handleDismissCreateMap}
                    />
                  : null} */}
          {providerMaps.map(providerMap =>
            <ProvidersTableRow
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
