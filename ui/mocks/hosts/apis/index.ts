import {layout, hosts} from 'test/resources'

export let getCpuAndLoadForHosts = jest.fn().mockResolvedValue(hosts)
export const getAllHosts = () => Promise.resolve()
export const getLayouts = () => Promise.resolve({data: {layouts: [layout]}})
export const getAppsForHosts = () => Promise.resolve(hosts)
export const getMeasurementsForHost = () => Promise.resolve()
