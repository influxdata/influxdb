import React, {PureComponent} from 'react'

export interface InjectedHoverProps {
  hoverTime: number | null
  activeViewID: string | null
  onSetHoverTime: (hoverTime: number | null) => void
  onSetActiveViewID: (activeViewID: string) => void
}

const {Provider, Consumer} = React.createContext<InjectedHoverProps>(null)

export class HoverTimeProvider extends PureComponent<{}, InjectedHoverProps> {
  public state: InjectedHoverProps = {
    hoverTime: null,
    activeViewID: null,
    onSetHoverTime: (hoverTime: number | null) => this.setState({hoverTime}),
    onSetActiveViewID: (activeViewID: string | null) =>
      this.setState({activeViewID}),
  }

  public render() {
    return <Provider value={this.state}>{this.props.children}</Provider>
  }
}

type Omit<T, V> = Pick<T, Exclude<keyof T, keyof V>>

export const withHoverTime = <P extends InjectedHoverProps>(
  Component: React.ComponentType<P>
) => (props: Omit<P, InjectedHoverProps>) => (
  <Consumer>
    {hoverTimeProps => <Component {...props} {...hoverTimeProps} />}
  </Consumer>
)
