// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Actions
import {getDashboard} from 'src/dashboards/actions/thunks'

// Types
import {AppState, ResourceType} from 'src/types'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {TechnoSpinner, SpinnerContainer} from '@influxdata/clockface'

// Selectors
import {getResourceStatus} from 'src/resources/selectors/getResourceStatus'

export interface Resource {
  type: ResourceType
  id: string
}

interface OwnProps {
  resources: Resource[]
  children: React.ReactNode
}

type ReduxProps = ConnectedProps<typeof connector>
export type Props = ReduxProps & OwnProps

@ErrorHandling
class GetResource extends PureComponent<Props> {
  controller = new AbortController()

  public componentDidMount() {
    const {resources} = this.props
    const promises = []
    resources.forEach(resource => {
      promises.push(this.getResourceDetails(resource))
    })
    Promise.all(promises)
  }

  public componentWillUnmount() {
    this.controller.abort()
  }

  private getResourceDetails({type, id}: Resource) {
    switch (type) {
      case ResourceType.Dashboards: {
        return this.props.getDashboard(id, this.controller.signal)
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

const mstp = (state: AppState, props: OwnProps) => {
  const remoteDataState = getResourceStatus(state, props.resources)

  return {
    remoteDataState,
  }
}

const mdtp = {
  getDashboard: getDashboard,
}

const connector = connect(mstp, mdtp)

export default connector(GetResource)
