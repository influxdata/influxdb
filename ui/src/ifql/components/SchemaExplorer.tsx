import React, {PureComponent} from 'react'
import PropTypes from 'prop-types'
import DatabaseList from 'src/ifql/components/DatabaseList'

interface State {
  db: string
}

const {shape} = PropTypes

class SchemaExplorer extends PureComponent<{}, State> {
  public static contextTypes = {
    source: shape({
      links: shape({}).isRequired,
    }).isRequired,
  }

  constructor(props) {
    super(props)
    this.state = {
      db: '',
    }
  }

  public render() {
    return (
      <div className="ifql-schema-explorer">
        <DatabaseList
          db={this.state.db}
          source={this.context.source}
          onChooseDatabase={this.handleChooseDatabase}
        />
      </div>
    )
  }

  private handleChooseDatabase = (db: string): void => {
    this.setState({db})
  }
}

export default SchemaExplorer
