import React, {PureComponent} from 'react'
import DatabaseList from 'src/ifql/components/DatabaseList'

class SchemaExplorer extends PureComponent {
  public render() {
    return (
      <div className="ifql-schema-explorer">
        <div className="ifql-schema--controls">
          <div className="ifql-schema--filter">
            <input
              className="form-control input-sm"
              placeholder="Filter YO schema dawg..."
              type="text"
              spellCheck={false}
              autoComplete="off"
            />
          </div>
          <button
            className="btn btn-sm btn-default btn-square"
            disabled={true}
            title="Collapse YO tree"
          >
            <span className="icon collapse" />
          </button>
        </div>
        <DatabaseList />
      </div>
    )
  }
}

export default SchemaExplorer
