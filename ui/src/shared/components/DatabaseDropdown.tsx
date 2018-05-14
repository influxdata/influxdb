import React, {Component} from 'react'
import Dropdown from 'src/shared/components/Dropdown'

import {showDatabases} from 'src/shared/apis/metaQuery'
import parsers from 'src/shared/parsing'
import {Source} from 'src/types/sources'
import {ErrorHandling} from 'src/shared/decorators/errors'
const {databases: showDatabasesParser} = parsers

interface Database {
  text: string
}

interface Props {
  database: string
  onSelectDatabase: (database: Database) => void
  onStartEdit?: () => void
  onErrorThrown: (error: string) => void
  source: Source
}

interface State {
  databases: Database[]
}

@ErrorHandling
class DatabaseDropdown extends Component<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      databases: [],
    }
  }

  public componentDidMount() {
    this.getDatabasesAsync()
  }

  public render() {
    const {databases} = this.state
    const {database, onSelectDatabase, onStartEdit} = this.props

    if (!database) {
      this.componentDidMount()
    }

    return (
      <Dropdown
        items={databases.map(text => ({
          text,
        }))}
        selected={database || 'Loading...'}
        onChoose={onSelectDatabase}
        onClick={onStartEdit ? onStartEdit : null}
      />
    )
  }

  private getDatabasesAsync = async (): Promise<void> => {
    const {source, database, onSelectDatabase, onErrorThrown} = this.props
    const proxy = source.links.proxy
    try {
      const {data} = await showDatabases(proxy)
      const {databases, errors} = showDatabasesParser(data)
      if (errors.length > 0) {
        throw errors[0] // only one error can come back from this, but it's returned as an array
      }

      const nonSystemDatabases = databases.filter(name => name !== '_internal')

      this.setState({
        databases: nonSystemDatabases,
      })
      const selectedDatabaseText = nonSystemDatabases.includes(database)
        ? database
        : nonSystemDatabases[0] || 'No databases'
      onSelectDatabase({
        text: selectedDatabaseText,
      })
    } catch (error) {
      console.error(error)
      onErrorThrown(error)
    }
  }
}

export default DatabaseDropdown
