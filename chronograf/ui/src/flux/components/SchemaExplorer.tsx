import React, {PureComponent} from 'react'

import DatabaseList from 'src/flux/components/DatabaseList'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import {Service} from 'src/types'

interface Props {
  service: Service
}

class SchemaExplorer extends PureComponent<Props> {
  public render() {
    const {service} = this.props
    return (
      <div className="flux-schema-explorer">
        <FancyScrollbar>
          <DatabaseList service={service} />
        </FancyScrollbar>
      </div>
    )
  }
}

export default SchemaExplorer
