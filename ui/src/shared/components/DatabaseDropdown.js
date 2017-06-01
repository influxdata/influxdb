import React, {PropTypes, Component} from 'react'
import Dropdown from 'shared/components/Dropdown'

import {showDatabases} from 'shared/apis/metaQuery'
import parsers from 'shared/parsing'
const {databases: showDatabasesParser} = parsers

class DatabaseDropdown extends Component {
  constructor(props) {
    super(props)
    this.state = {
      databases: [],
    }

    this._getDatabases = ::this._getDatabases
  }

  componentDidMount() {
    this._getDatabases()
  }

  render() {
    const {databases} = this.state
    const {database, onSelectDatabase, onStartEdit} = this.props

    if (!database) {
      this.componentDidMount()
    }

    return (
      <Dropdown
        items={databases.map(text => ({text}))}
        selected={database || 'Loading...'}
        onChoose={onSelectDatabase}
        onClick={onStartEdit ? () => onStartEdit(null) : null}
      />
    )
  }

  async _getDatabases() {
    const {source} = this.context
    const {database, onSelectDatabase, onErrorThrown} = this.props
    const proxy = source.links.proxy
    try {
      const {data} = await showDatabases(proxy)
      const {databases} = showDatabasesParser(data)

      this.setState({databases})
      const selectedDatabaseText = databases.includes(database)
        ? database
        : databases[0] || 'No databases'
      onSelectDatabase({text: selectedDatabaseText})
    } catch (error) {
      console.error(error)
      onErrorThrown(error)
    }
  }
}

const {func, shape, string} = PropTypes

DatabaseDropdown.contextTypes = {
  source: shape({
    links: shape({
      proxy: string.isRequired,
    }).isRequired,
  }).isRequired,
}

DatabaseDropdown.propTypes = {
  database: string,
  onSelectDatabase: func.isRequired,
  onStartEdit: func,
  onErrorThrown: func.isRequired,
}

export default DatabaseDropdown
