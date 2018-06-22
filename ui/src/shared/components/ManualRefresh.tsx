import React, {Component, ComponentClass} from 'react'

export interface ManualRefreshProps {
  manualRefresh: number
  onManualRefresh: () => void
}

interface ManualRefreshState {
  manualRefresh: number
}

function ManualRefresh<P>(
  WrappedComponent: ComponentClass<P & ManualRefreshProps>
): ComponentClass<P> {
  return class extends Component<P & ManualRefreshProps, ManualRefreshState> {
    public constructor(props: P & ManualRefreshProps) {
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
}

export default ManualRefresh
