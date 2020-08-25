// Libraries
import React, {PureComponent} from 'react'
import {MarkdownRenderer} from 'src/shared/components/views/MarkdownRenderer'
import {fetchReadMe} from 'src/templates/api'

interface Props {
  directory: string
}

const cloudImageRenderer = (): any =>
  "We don't support images in markdown for security purposes"

export class CommunityTemplateReadme extends PureComponent<Props> {
  state = {readMeData: ''}
  componentDidMount = async () => {
    try {
      const response = await fetchReadMe(this.props.directory)
      this.setState({readMeData: response})
    } catch (error) {
      console.error(
        'There has been a problem with your fetch operation:',
        error
      )
      this.setState({readMeData: '## No read me to display!'})
    }
  }

  render = () => {
    return (
      <MarkdownRenderer
        text={this.state.readMeData}
        className="markdown-format"
        cloudRenderers={{
          image: cloudImageRenderer,
          imageReference: cloudImageRenderer,
        }}
      />
    )
  }
}
