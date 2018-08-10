// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import DatabaseList from 'src/flux/components/DatabaseList'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Types
import {Source} from 'src/types/v2'
import {NotificationAction} from 'src/types/notifications'

interface Props {
  source: Source
  notify: NotificationAction
}

class SchemaExplorer extends PureComponent<Props> {
  public render() {
    const {source, notify} = this.props

    return (
      <div className="flux-schema-explorer">
        <FancyScrollbar>
          <DatabaseList source={source} notify={notify} />
        </FancyScrollbar>
      </div>
    )
  }
}

const mdtp = {
  notify: notifyAction,
}

export default connect(null, mdtp)(SchemaExplorer)
