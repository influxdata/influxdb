import React, {Component, ComponentClass} from 'react'

export interface ManualRefreshProps {
  manualRefresh: number
  onManualRefresh: () => void
}

interface ManualRefreshState {
  manualRefresh: number
}

const ManualRefresh = <P extends ManualRefreshProps>(
  WrappedComponent: ComponentClass<P>
): ComponentClass<P> =>
  class extends Component<P, ManualRefreshState> {
    public constructor(props: P) {
      super(props)
      this.state = {
        manualRefresh: Date.now(),
      }
    }

    public render() {
      return (
        <WrappedComponent
          {...this.props}
          manualRefresh={this.state.manualRefresh}
          onManualRefresh={this.handleManualRefresh}
        />
      )
    }

    private handleManualRefresh = (): void => {
      this.setState({
        manualRefresh: Date.now(),
      })
    }
  }

export default ManualRefresh
