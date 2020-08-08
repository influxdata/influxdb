// Constants
import {CLIENT_LIBS} from 'src/shared/constants/routes'

// Types
import {WriteDataItem, WriteDataSection} from 'src/writeData/constants'

// Markdown
import ArduinoMarkdown from 'src/writeData/components/clientLibraries/Arduino.md'
import CSharpMarkdown from 'src/writeData/components/clientLibraries/CSharp.md'
import GoMarkdown from 'src/writeData/components/clientLibraries/Go.md'
import JavaMarkdown from 'src/writeData/components/clientLibraries/Java.md'
import NodeMarkdown from 'src/writeData/components/clientLibraries/Node.md'
import PythonMarkdown from 'src/writeData/components/clientLibraries/Python.md'
import RubyMarkdown from 'src/writeData/components/clientLibraries/Ruby.md'
import PHPMarkdown from 'src/writeData/components/clientLibraries/PHP.md'
import KotlinMarkdown from 'src/writeData/components/clientLibraries/Kotlin.md'
import ScalaMarkdown from 'src/writeData/components/clientLibraries/Scala.md'

// Graphics
import arduinoLogo from 'src/writeData/graphics/arduinoLogo.svg'
import csharpLogo from 'src/writeData/graphics/csharpLogo.svg'
import goLogo from 'src/writeData/graphics/goLogo.svg'
import javaLogo from 'src/writeData/graphics/javaLogo.svg'
import nodeLogo from 'src/writeData/graphics/nodeLogo.svg'
import pythonLogo from 'src/writeData/graphics/pythonLogo.svg'
import rubyLogo from 'src/writeData/graphics/rubyLogo.svg'
import phpLogo from 'src/writeData/graphics/phpLogo.svg'
import kotlinLogo from 'src/writeData/graphics/kotlinLogo.svg'
import scalaLogo from 'src/writeData/graphics/scalaLogo.svg'

export const WRITE_DATA_CLIENT_LIBRARIES: WriteDataItem[] = [
  {
    id: 'arduino',
    name: 'Arduino',
    url: `${CLIENT_LIBS}/arduino`,
    image: arduinoLogo,
    markdown: ArduinoMarkdown,
  },
  {
    id: 'csharp',
    name: 'C#',
    url: `${CLIENT_LIBS}/csharp`,
    image: csharpLogo,
    markdown: CSharpMarkdown,
  },
  {
    id: 'go',
    name: 'GO',
    url: `${CLIENT_LIBS}/go`,
    image: goLogo,
    markdown: GoMarkdown,
  },
  {
    id: 'java',
    name: 'Java',
    url: `${CLIENT_LIBS}/java`,
    image: javaLogo,
    markdown: JavaMarkdown,
  },
  {
    id: 'javascript-node',
    name: 'JavaScript/Node.js',
    url: `${CLIENT_LIBS}/javascript-node`,
    image: nodeLogo,
    markdown: NodeMarkdown,
  },
  {
    id: 'python',
    name: 'Python',
    url: `${CLIENT_LIBS}/python`,
    image: pythonLogo,
    markdown: PythonMarkdown,
  },
  {
    id: 'ruby',
    name: 'Ruby',
    url: `${CLIENT_LIBS}/ruby`,
    image: rubyLogo,
    markdown: RubyMarkdown,
  },
  {
    id: 'php',
    name: 'PHP',
    url: `${CLIENT_LIBS}/php`,
    image: phpLogo,
    markdown: PHPMarkdown,
  },
  {
    id: 'kotlin',
    name: 'Kotlin',
    url: `${CLIENT_LIBS}/kotlin`,
    image: kotlinLogo,
    markdown: KotlinMarkdown,
  },
  {
    id: 'scala',
    name: 'Scala',
    url: `${CLIENT_LIBS}/scala`,
    image: scalaLogo,
    markdown: ScalaMarkdown,
  },
]

const WRITE_DATA_CLIENT_LIBRARIES_SECTION: WriteDataSection = {
  id: CLIENT_LIBS,
  name: 'Client Libraries',
  description: 'Back-end, front-end, and mobile applications',
  items: WRITE_DATA_CLIENT_LIBRARIES,
}

export default WRITE_DATA_CLIENT_LIBRARIES_SECTION
