import React, {PropTypes, Component} from 'react'
import Dropdown from 'shared/components/Dropdown'

import {showDatabases} from 'shared/apis/metaQuery'
import showDatabasesParser from 'shared/parsing/showDatabases'

class DatabaseDropdown extends Component {
  constructor(props) {
    super(props)
    this.state = {
      databases: [],
    }
  }

  componentDidMount() {
    const {source} = this.context
    const {database, onSelectDatabase} = this.props
    const proxy = source.links.proxy
    showDatabases(proxy).then(resp => {
      const {databases} = showDatabasesParser(resp.data)
      this.setState({databases})
      const selected = databases.includes(database)
        ? database
        : databases[0] || 'No databases'
      onSelectDatabase({text: selected})
    })
  }

  render() {
    const {databases} = this.state
    const {database, onSelectDatabase, onStartEdit} = this.props

    // :(
    if (!database) {
      this.componentDidMount()
    }

    return (
      <Dropdown
        items={databases.map(text => ({text}))}
        selected={database || 'Loading...'}
        onChoose={onSelectDatabase}
        onClick={() => onStartEdit(null)}
      />
    )
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
  onStartEdit: func.isRequired,
}

export default DatabaseDropdown
