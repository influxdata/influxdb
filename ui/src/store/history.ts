import {createBrowserHistory} from 'history'
import {getBrowserBasepath} from 'src/utils/basepath'

const basepath = getBrowserBasepath()
// Older method used for pre-IE 11 compatibility
window.basepath = basepath

export const history = createBrowserHistory({basename: basepath})
