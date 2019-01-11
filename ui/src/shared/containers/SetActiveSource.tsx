// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Actions
import {setActiveSource} from 'src/sources/actions'

// Utils
import {getSources, getActiveSource} from 'src/sources/selectors'
import {readQueryParams, updateQueryParams} from 'src/shared/utils/queryParams'
import {getDeep} from 'src/utils/wrappers'

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

    const querySourceID = readQueryParams().sourceID

    let resolvedSourceID: string

    if (sources.find(s => s.id === activeSourceID)) {
      resolvedSourceID = activeSourceID
    } else if (sources.find(s => s.id === querySourceID)) {
      resolvedSourceID = querySourceID
    } else if (sources.length) {
      resolvedSourceID = getDeep<string>(sources, '0.id', '')
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
