import React, {PureComponent} from 'react'
import DataExplorer from './DataExplorer'

import {Source} from 'src/types'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  source: Source
}

@ErrorHandling
class DataExplorerPage extends PureComponent<Props> {
  public render() {
    return (
      <div className="page">
        <DataExplorer source={this.props.source} />
      </div>
    )
  }
}

export default DataExplorerPage
