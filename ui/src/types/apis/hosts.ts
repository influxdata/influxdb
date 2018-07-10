import {HostNames} from 'src/types/hosts'
import {Source} from 'src/types/sources'

export type GetAllHosts = (source: Source) => Promise<HostNames>
