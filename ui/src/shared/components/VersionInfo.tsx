// Libraries
import React, {PureComponent} from 'react'

// Constants
import {VERSION, GIT_SHA} from 'src/shared/constants'

interface Props {
  widthPixels?: number
}

class VersionInfo extends PureComponent<Props> {
  public render() {
    return (
      <div className="version-info" style={this.style}>
        <p>
          Version {VERSION} {GIT_SHA && <code>({GIT_SHA.slice(0, 7)})</code>}
        </p>
      </div>
    )
  }

  private get style() {
    if (this.props.widthPixels) {
      return {width: `${this.props.widthPixels}px`}
    }
  }
}

export default VersionInfo
