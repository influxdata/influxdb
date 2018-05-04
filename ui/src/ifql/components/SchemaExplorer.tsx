import React, {PureComponent} from 'react'
import DatabaseList from 'src/ifql/components/DatabaseList'

class SchemaExplorer extends PureComponent {
  public render() {
    return (
      <div className="ifql-schema-explorer">
        <DatabaseList />
      </div>
    )
  }
}

export default SchemaExplorer
