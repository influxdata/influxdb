// Libraries
import React, {PureComponent} from 'react'
import {MarkdownRenderer} from 'src/shared/components/views/MarkdownRenderer'

interface Props{directory:string}

const cloudImageRenderer = (): any =>
  "We don't support images in markdown for security purposes"

export class CommunityTemplateReadme extends PureComponent<Props>{
  state = {readMeData: ''}
  componentDidMount = async () => {
    try{
      const response =  await fetch(`https://raw.githubusercontent.com/influxdat/community-templates/master/${this.props.directory}/README.md`)
        if (!response.ok) {
          throw new Error('Network response was not ok');
        }
        const readMeData = await response.text()
        this.setState({readMeData: readMeData})

    }catch(error){
      console.error('There has been a problem with your fetch operation:', error);
      this.setState({readMeData: "## No read me to display!"})
    };
  }

  render =() => {
    return (<MarkdownRenderer
      text={this.state.readMeData}
      className="markdown-format"
      cloudRenderers={{
        image: cloudImageRenderer,
        imageReference: cloudImageRenderer,
      }}
    />)
  }

}
