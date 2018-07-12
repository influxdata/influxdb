import {PureComponent} from 'react'

interface Props {
  children: JSX.Element
  autoRefresh: number
}

class AutoRefresh extends PureComponent<Props> {
  private intervalID: NodeJS.Timer

  public state = {
    now: new Date(),
  }

  public async componentDidMount() {
    this.startNewPolling()
  }

  public async componentDidUpdate(prevProps: Props) {
    const {autoRefresh} = this.props

    if (autoRefresh !== prevProps.autoRefresh) {
      return
    }

    this.startNewPolling()
  }

  public componentWillUnmount() {
    this.clearInterval()
  }

  public render() {
    return this.props.children(this.state.now)
  }

  private startNewPolling() {
    this.clearInterval()

    const {autoRefresh} = this.props

    if (autoRefresh) {
      this.intervalID = setInterval(this.update, autoRefresh)
    }
  }

  private update = (): void => {
    this.setState({now: new Date()})
  }

  private clearInterval() {
    if (!this.intervalID) {
      return
    }

    clearInterval(this.intervalID)
    this.intervalID = null
  }
}

export default AutoRefresh
