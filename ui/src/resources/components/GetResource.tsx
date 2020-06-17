// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Actions
import {getDashboard} from 'src/dashboards/actions/thunks'

// Types
import {AppState, RemoteDataState, ResourceType} from 'src/types'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {TechnoSpinner, SpinnerContainer} from '@influxdata/clockface'

// Selectors
import {getResourceStatus} from 'src/resources/selectors/getResourceStatus'

interface StateProps {
  remoteDataState: RemoteDataState
}

interface DispatchProps {
  getDashboard: typeof getDashboard
}

export interface Resource {
  type: ResourceType
  id: string
}

interface OwnProps {
  resources: Resource[]
}

export type Props = StateProps & DispatchProps & OwnProps

@ErrorHandling
class GetResource extends PureComponent<Props, StateProps> {
  public componentDidMount() {
    const {resources} = this.props
    const promises = []
    resources.forEach(resource => {
      promises.push(this.getResourceDetails(resource))
    })
    Promise.all(promises)
  }

  private getResourceDetails({type, id}: Resource) {
    switch (type) {
      case ResourceType.Dashboards: {
        return this.props.getDashboard(id)
      }

      default: {
        throw new Error(
          `incorrect resource type: "${type}" provided to GetResources`
        )
      }
    }
  }

  public render() {
    const {children, remoteDataState} = this.props

    return (
      <>
        <SpinnerContainer
          loading={remoteDataState}
          spinnerComponent={<TechnoSpinner />}
          testID="dashboard-container--spinner"
        >
          {children}
        </SpinnerContainer>
      </>
    )
  }
}

const mstp = (state: AppState, props: Props): StateProps => {
  const remoteDataState = getResourceStatus(state, props.resources)

  return {
    remoteDataState,
  }
}

const mdtp = {
  getDashboard: getDashboard,
}

export default connect<StateProps, DispatchProps, {}>(mstp, mdtp)(GetResource)
