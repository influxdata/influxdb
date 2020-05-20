import {register} from 'src/notebooks'
import MarkdownPanel from './MarkdownPanel'
import './style.scss'

export type MarkdownMode = 'edit' | 'preview'

register({
  type: 'markdown',
  component: MarkdownPanel,
  button: 'Markdown',
  initial: () => ({
    text: 'Content',
    mode: 'preview',
  }),
})
