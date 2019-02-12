// Libraries
import React, {Component, ReactElement, ReactNode} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import OrganizationNavigation from 'src/organizations/components/OrganizationNavigation'
import {Tabs} from 'src/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface OwnProps {
  name: string
  avatar?: string
  description?: string
  children: ReactNode[] | ReactNode
  activeTabUrl: string
  orgID: string
}

type Props = OwnProps & WithRouterProps

@ErrorHandling
class OrganizationTabs extends Component<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
    return (
      <Tabs>
        <OrganizationNavigation
          tab={this.props.activeTabUrl}
          orgID={this.props.orgID}
        />
        <Tabs.TabContents>{this.activeSectionComponent}</Tabs.TabContents>
      </Tabs>
    )
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
}

export default withRouter<OwnProps>(OrganizationTabs)
