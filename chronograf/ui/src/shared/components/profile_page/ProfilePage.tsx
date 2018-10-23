// Libraries
import React, {Component, ReactElement, ReactNode} from 'react'
import {withRouter, InjectedRouter} from 'react-router'

// Components
import ProfilePageSection from 'src/shared/components/profile_page/ProfilePageSection'
import ProfilePageTab from 'src/shared/components/profile_page/ProfilePageTab'
import ProfilePageHeader from 'src/shared/components/profile_page/ProfilePageHeader'
import Avatar from 'src/shared/components/avatar/Avatar'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  name: string
  avatar: string
  description?: string
  children: ReactNode[]
  activeTabUrl: string
  router: InjectedRouter
  parentUrl: string
}

@ErrorHandling
class ProfilePage extends Component<Props> {
  public static Section = ProfilePageSection
  public static Header = ProfilePageHeader

  constructor(props) {
    super(props)
  }

  public render() {
    this.validateChildTypes()

    return (
      <div className="profile">
        <div className="profile-nav">
          {this.profileNavHeader}
          {this.profileNavTabs}
        </div>
        <div className="profile-content">{this.activeSectionComponent}</div>
      </div>
    )
  }

  private get profileNavTabs(): JSX.Element {
    const {children, activeTabUrl} = this.props

    return (
      <div className="profile-nav--tabs">
        {React.Children.map(children, (child: JSX.Element) => (
          <ProfilePageTab
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

  private get profileNavHeader(): JSX.Element {
    const {avatar, name, description} = this.props

    return (
      <div className="profile-nav--header">
        <Avatar
          imageURI={avatar}
          diameterPixels={160}
          customClass="profile-nav--avatar"
        />
        <h3 className="profile-nav--name">{name}</h3>
        {description && (
          <p className="profile-nav--description">{description}</p>
        )}
      </div>
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

  public handleTabClick = (url: string) => (): void => {
    const {router, parentUrl} = this.props
    router.push(`${parentUrl}/${url}/`)
  }

  private validateChildTypes = (): void => {
    const {children} = this.props

    React.Children.forEach(children, (child: JSX.Element) => {
      if (child.type !== ProfilePageSection) {
        throw new Error(
          '<ProfilePage> expected children of type <ProfilePage.Section />'
        )
      }
    })
  }
}

export default withRouter(ProfilePage)
