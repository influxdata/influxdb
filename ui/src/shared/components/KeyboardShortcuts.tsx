import {PureComponent, ReactNode} from 'react'

interface Props {
  children: ReactNode
  onControlEnter: () => void
}

class KeyboardShortcuts extends PureComponent<Props> {
  public componentWillMount() {
    document.addEventListener('keydown', this.handleKeyboardShortcuts)
  }

  public componentWillUnmount() {
    document.removeEventListener('keydown', this.handleKeyboardShortcuts)
  }

  public render() {
    return this.props.children
  }

  private handleKeyboardShortcuts = (e: KeyboardEvent) => {
    if (e.ctrlKey && e.key === 'Enter') {
      this.props.onControlEnter()
      e.preventDefault()
    }
  }
}

export default KeyboardShortcuts
