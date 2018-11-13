// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Actions
import {setActiveSource} from 'src/sources/actions'

// Utils
import {getSources, getActiveSource} from 'src/sources/selectors'
import {readQueryParams, updateQueryParams} from 'src/shared/utils/queryParams'

// Types
import {Source, AppState} from 'src/types/v2'

interface PassedInProps {
  children: React.ReactElement<any>
}

interface ConnectStateProps {
  activeSourceID: string
  sources: Source[]
  source: Source
}

interface ConnectDispatchProps {
  onSetActiveSource: typeof setActiveSource
}

type Props = ConnectStateProps & ConnectDispatchProps & PassedInProps

class SetActiveSource extends PureComponent<Props> {
  public componentDidMount() {
    this.resolveActiveSource()
  }

  public render() {
    const {source} = this.props

    if (!source) {
      return null
    }

    return this.props.children
  }

  public componentDidUpdate() {
    this.resolveActiveSource()
  }

  private resolveActiveSource() {
    const {sources, activeSourceID, onSetActiveSource} = this.props

    const defaultSourceID = get(sources.find(s => s.default), 'id')
    const querySourceID = readQueryParams().sourceID

    let resolvedSourceID

    if (sources.find(s => s.id === activeSourceID)) {
      resolvedSourceID = activeSourceID
    } else if (sources.find(s => s.id === querySourceID)) {
      resolvedSourceID = querySourceID
    } else if (defaultSourceID) {
      resolvedSourceID = defaultSourceID
    } else if (sources.length) {
      resolvedSourceID = sources[0]
    } else {
      throw new Error('no source exists')
    }

    if (activeSourceID !== resolvedSourceID) {
      onSetActiveSource(resolvedSourceID)
    }

    if (querySourceID !== resolvedSourceID) {
      updateQueryParams({sourceID: resolvedSourceID})
    }
  }
}

const mstp = (state: AppState) => ({
  source: getActiveSource(state),
  sources: getSources(state),
  activeSourceID: state.sources.activeSourceID,
})

const mdtp = {
  onSetActiveSource: setActiveSource,
}

export default connect<ConnectStateProps, ConnectDispatchProps, PassedInProps>(
  mstp,
  mdtp
)(SetActiveSource)
