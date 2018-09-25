import React, {PureComponent} from 'react'
import {Link} from 'react-router'
import {stripPrefix} from 'src/utils/basepath'

import {Source} from 'src/types/v2'

interface Props {
  source: Source
  currentSource: Source
}

class ConnectionLink extends PureComponent<Props> {
  public render() {
    const {source} = this.props
    return (
      <h5 className="margin-zero">
        <Link
          to={`${stripPrefix(location.pathname)}/${source.id}/edit?${
            this.sourceParam
          }`}
          className={this.className}
        >
          <strong>{source.name}</strong>
          {this.default}
        </Link>
      </h5>
    )
  }

  private get sourceParam(): string {
    const {currentSource} = this.props

    return `sourceID=${currentSource.id}`
  }

  private get className(): string {
    if (this.isCurrentSource) {
      return 'link-success'
    }

    return ''
  }

  private get default(): string {
    const {source} = this.props
    if (source.default) {
      return ' (Default)'
    }

    return ''
  }

  private get isCurrentSource(): boolean {
    const {source, currentSource} = this.props
    return source.id === currentSource.id
  }
}

export default ConnectionLink
