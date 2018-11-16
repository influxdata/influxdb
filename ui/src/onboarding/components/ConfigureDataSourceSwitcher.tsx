// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

export interface Props {
  dataSource: string
}

@ErrorHandling
class ConfigureDataSourceSwitcher extends PureComponent<Props> {
  public render() {
    const {dataSource} = this.props

    return <div>{dataSource}</div>
  }
}

export default ConfigureDataSourceSwitcher
