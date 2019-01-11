// Libraries
import React, {SFC, Component} from 'react'
import ReactDOM from 'react-dom'

// Components
import RightClickMenu from 'src/clockface/components/right_click_menu/RightClickMenu'
import RightClickMenuItem from 'src/clockface/components/right_click_menu/RightClickMenuItem'
import {ClickOutside} from 'src/shared/components/ClickOutside'
import Select from 'src/clockface/components/Select'

interface Props {
  className?: string
  children: JSX.Element[]
}

interface ChildProps {
  children: JSX.Element
}
interface State {
  expanded: boolean
  mouseX: number
  mouseY: number
}

/**
 * Handles bubbling onContextMenu events from elements
 *  wrapped in Trigger and displays the MenuContainer
 *  contents. Portals the RightClickMenu into the
 *  RightClickLayer.
 *
 * @example Using RightClickMenu and RightClickMenuItem
 *
 *   <RightClick>
 *     <RightClick.Trigger>
 *       <button>Right click me</button>
 *     </RightClick.Trigger>
 *     <RightClick.MenuContainer>
 *       <RightClick.Menu>
 *         <RightClick.MenuItem onClick={this.handleClickA}>
 *           Test Item A
 *         </RightClick.MenuItem>
 *         <RightClick.MenuItem onClick={this.handleClickB}>
 *           Test Item B
 *         </RightClick.MenuItem>
 *         <RightClick.MenuItem
 *           onClick={this.handleClickC}
 *           disabled={true}
 *         >
 *           Test Item B
 *         </RightClick.MenuItem>
 *       </RightClick.Menu>
 *     </RightClick.MenuContainer>
 *   </RightClick>
 *
 * @example Using a custom menu
 *
 *   <RightClick>
 *     <RightClick.Trigger>
 *       <button>Right click me</button>
 *     </RightClick.Trigger>
 *     <RightClick.MenuContainer>
 *        <ul className="custom-menu">
 *           <li onClick={this.handleClickA}>Test A</li>
 *           <li onClick={this.handleClickB}>Test B</li>
 *        </ul>
 *     </RightClick.MenuContainer>
 *   </RightClick>
 */

class RightClick extends Component<Props, State> {
  public static Menu = RightClickMenu
  public static MenuItem = RightClickMenuItem
  public static MenuContainer: SFC<ChildProps> = ({children}) => children
  public static Trigger: SFC<ChildProps> = ({children}) => children

  private static Only: typeof Select = props => <Select count={1} {...props} />

  public state: State = {
    expanded: false,
    mouseX: null,
    mouseY: null,
  }

  public componentDidMount() {
    document.addEventListener('contextmenu', this.handleRightClick, true)
  }

  public componentWillUnmount() {
    document.removeEventListener('contextmenu', this.handleRightClick, true)
  }

  public render() {
    this.validateChildren()

    return (
      <>
        <RightClick.Only type={RightClick.Trigger}>
          {this.props.children}
        </RightClick.Only>
        <div className="right-click--wrapper">{this.menu}</div>
      </>
    )
  }

  private handleRightClick = (e: MouseEvent): void => {
    const domNode = ReactDOM.findDOMNode(this)

    if (!domNode || domNode.contains(e.target)) {
      e.preventDefault()

      const {pageX: mouseX, pageY: mouseY} = e

      this.setState({
        expanded: true,
        mouseX,
        mouseY,
      })
    }
  }

  private handleCollapseMenu = (): void => {
    this.setState({expanded: false})
  }

  private get menu() {
    return ReactDOM.createPortal(
      this.menuElement,
      document.getElementById('right-click--layer')
    )
  }

  private get menuElement(): JSX.Element {
    const {expanded, mouseX, mouseY} = this.state

    if (!expanded) {
      return null
    }

    return (
      <div
        style={{
          position: 'fixed',
          left: mouseX,
          top: mouseY,
        }}
        onClick={this.handleCollapseMenu}
      >
        <ClickOutside onClickOutside={this.handleCollapseMenu}>
          <RightClick.Only type={RightClick.MenuContainer}>
            {this.props.children}
          </RightClick.Only>
        </ClickOutside>
      </div>
    )
  }

  private validateChildren = (): void => {
    const {children} = this.props

    if (React.Children.count(children) > 2) {
      throw new Error('<RightClick> has more than 2 children')
    }
  }
}

export default RightClick
