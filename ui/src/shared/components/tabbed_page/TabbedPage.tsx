// Libraries
import React, {Component, ReactElement, ReactNode} from 'react'
import {withRouter, WithRouterProps} from 'react-router-dom'

// Components
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import TabbedPageTab from 'src/shared/components/tabbed_page/TabbedPageTab'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface OwnProps {
  name: string
  avatar?: string
  description?: string
  children: ReactNode[] | ReactNode
  activeTabUrl: string
  parentUrl: string
}

type Props = OwnProps & WithRouterProps

@ErrorHandling
class TabbedPage extends Component<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
    this.validateChildTypes()

    return (
      <div className="tabbed-page">
        <div className="tabbed-page-nav">
          {this.navHeader}
          {this.navTabs}
        </div>
        <div className="tabbed-page-content">{this.activeSectionComponent}</div>
      </div>
    )
  }

  private get navTabs(): JSX.Element {
    const {children, activeTabUrl} = this.props

    return (
      <div className="tabbed-page-nav--tabs">
        {React.Children.map(children, (child: JSX.Element) => (
          <TabbedPageTab
            title={child.props.title}
            key={child.props.id}
            id={child.props.id}
            url={child.props.url}
            active={child.props.url === activeTabUrl}
            onClick={this.handleTabClick}
          />
        ))}
      </div>
    )
  }

  private get navHeader(): JSX.Element {
    const {description} = this.props

    if (description) {
      return (
        <div className="tabbed-page-nav--header">
          <p className="tabbed-page-nav--description">{description}</p>
        </div>
      )
    }
  }

  private get activeSectionComponent(): JSX.Element[] {
    const {children, activeTabUrl} = this.props

    // Using ReactElement as type to ensure children have props
    return React.Children.map(children, (child: ReactElement<any>) => {
      if (child.props.url === activeTabUrl) {
        return child.props.children
      }
    })
  }

  public handleTabClick = (url: string) => (): void => {
    const {router, parentUrl} = this.props
    router.push(`${parentUrl}/${url}/`)
  }

  private validateChildTypes = (): void => {
    const {children} = this.props

    React.Children.forEach(children, (child: JSX.Element) => {
      if (child.type !== TabbedPageSection) {
        throw new Error(
          '<TabbedPage> expected children of type <TabbedPageSection />'
        )
      }
    })
  }
}

export default withRouter<OwnProps>(TabbedPage)
