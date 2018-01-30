import React, {Component, PropTypes} from 'react'

import ProvidersTableRow from 'src/admin/components/chronograf/ProvidersTableRow'
import ProvidersTableRowNew from 'src/admin/components/chronograf/ProvidersTableRowNew'

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

  handleCancelCreateMap = () => {
    this.setState({isCreatingMap: false})
  }

  handleCreateMap = newMap => {
    const {onCreateMap} = this.props
    onCreateMap(newMap)
  }

  render() {
    const {mappings = [], organizations, onUpdateMap, onDeleteMap} = this.props
    const {isCreatingMap} = this.state

    const tableTitle =
      mappings.length === 1 ? '1 Map' : `${mappings.length} Maps`

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
            <div className="fancytable--th" />
            <div className="fancytable--th provider--delete" />
          </div>
          {mappings.map(mapping =>
            <ProvidersTableRow
              key={mapping.id}
              mapping={mapping}
              organizations={organizations}
              onDelete={onDeleteMap}
              onUpdate={onUpdateMap}
            />
          )}
          {isCreatingMap
            ? <ProvidersTableRowNew
                organizations={organizations}
                onCreate={this.handleCreateMap}
                onCancel={this.handleCancelCreateMap}
              />
            : null}
        </div>
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

ProvidersTable.propTypes = {
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
