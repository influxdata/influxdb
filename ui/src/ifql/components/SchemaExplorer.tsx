import React, {PureComponent} from 'react'

import DatabaseList from 'src/ifql/components/DatabaseList'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import {Service} from 'src/types'

interface Props {
  service: Service
}

class SchemaExplorer extends PureComponent<Props> {
  public render() {
    const {service} = this.props
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
        <FancyScrollbar>
          <DatabaseList service={service} />
        </FancyScrollbar>
      </div>
    )
  }
}

export default SchemaExplorer
